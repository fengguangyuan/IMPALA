Status HdfsScanNode::CreateAndPrepareScanner(HdfsPartitionDescriptor* partition,
    ScannerContext* context, scoped_ptr<HdfsScanner>* scanner) {
  DCHECK(context != NULL);
  THdfsCompression::type compression =
      context->GetStream()->file_desc()->file_compression;

  // Create a new scanner for this file format and compression.
  switch (partition->file_format()) {
    case THdfsFileFormat::TEXT:
      // Lzo-compressed text files are scanned by a scanner that it is implemented as a
      // dynamic library, so that Impala does not include GPL code.
      if (compression == THdfsCompression::LZO) {
        scanner->reset(HdfsLzoTextScanner::GetHdfsLzoTextScanner(this, runtime_state_));
      } else {
        scanner->reset(new HdfsTextScanner(this, runtime_state_));
      }
      break;
    case THdfsFileFormat::SEQUENCE_FILE:
      scanner->reset(new HdfsSequenceScanner(this, runtime_state_));
      break;
    case THdfsFileFormat::RC_FILE:
      scanner->reset(new HdfsRCFileScanner(this, runtime_state_));
      break;
    case THdfsFileFormat::AVRO:
      scanner->reset(new HdfsAvroScanner(this, runtime_state_));
      break;
    case THdfsFileFormat::PARQUET:
      {
        const Bitmap* bitmap_filter = 0;
        bool conjuncts_passed = true;
        uint32_t fragment_seed = -1;
        const vector<ExprContext*>& value_ctxs = partition->partition_key_value_ctxs();
        int tuple_offset = -1;
        uint32_t filters_size = 0;
        uint32_t tuple_size = tuple_desc_->slots().size();
        uint32_t partition_key_size = partition_key_slots_.size();
        Tuple* tuple;
        // If tuple_desc_ contains more slots than partition_key_slots_,
        // the 'else' block should be passed.
        // TODO:The sentence 'if (tuple_desc_->slots().size() > num_partition_keys()) {'
        // can work well?
        if (tuple_desc_->slots().size() > partition_key_slots_.size()) {
          conjuncts_passed = true;
        } else {
          filters_size = runtime_state_->slot_bitmap_filters_size();
          if (filters_size) {
            for (int i = 0; i < partition_key_slots_.size(); ++i) {
              const SlotDescriptor* slot_desc = partition_key_slots_[i];
              // Exprs guaranteed to be literals, so can safely be evaluated without a
              // row context
              void* value = value_ctxs[slot_desc->col_pos()]->GetValue(NULL);
              RawValue::Write(value, tuple, slot_desc, NULL);
            }
            // Check every slot in tuple_desc_. if it's value is not in bitmap_filter,
            // then conjuncts_passed is assigned 'false', or 'true'.
            BOOST_FOREACH(SlotDescriptor* slot, tuple_desc_->slots()) {
              if (!slot->is_materialized()) continue;
             // if (slot->parent() == tuple_desc_ && hdfs_table_->IsClusteringCol(slot)) {
              if (slot->parent() == tuple_desc_) {
                fragment_seed = runtime_state_->fragment_hash_seed();
                tuple_offset = slot->tuple_offset();
                bitmap_filter = runtime_state_->GetBitmapFilter(slot->id());
                if ((bitmap_filter != NULL)) {
                  uint32_t h = RawValue::GetHashValue(tuple->GetSlot(slot->tuple_offset()),
                                                      slot->type(), fragment_seed);
                  conjuncts_passed = bitmap_filter->Get<true>(h);
                  // if conjuncts_passed is false, it means this scanner should be skipped.
                  //if (!conjuncts_passed) break;
                  if (!conjuncts_passed) {
                    SetDone();
                    return Status::CANCELLED;
                  }
                }
                conjuncts_passed = true;
              }
            }
          }
        }
        // If 'conjuncts_passed = true' and 'filters_size > 0', the scan_range handled by
        // probe-side should be skiped. Or create a new scanner.
        // TODO: 'conjuncts_passed && filters_size' will skip the buid-side.
        //if (!conjuncts_passed && filters_size) {
        //if (!conjuncts_passed) {
        //  SetDone();
        //  return Status::CANCELLED;
        //} else {
          scanner->reset(new HdfsParquetScanner(this, runtime_state_));
          break;
        //}
      }
      //scanner->reset(new HdfsParquetScanner(this, runtime_state_));
      //break;
    default:
      return Status(Substitute("Unknown Hdfs file format type: $0",
          partition->file_format()));
  }
  DCHECK(scanner->get() != NULL);
  Status status = ExecDebugAction(TExecNodePhase::PREPARE_SCANNER, runtime_state_);
  if (status.ok()) status = scanner->get()->Prepare(context);
  if (!status.ok()) scanner->reset();
  return status;
}
 980 Status HdfsScanNode::ProcessSplit(DiskIoMgr::ScanRange* scan_range) { 
 981   DCHECK(scan_range != NULL); 
 982  
 983   ScanRangeMetadata* metadata = 
 984       reinterpret_cast<ScanRangeMetadata*>(scan_range->meta_data()); 
 985   int64_t partition_id = metadata->partition_id; 
 986   HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id); 
 987   DCHECK(partition != NULL) << "table_id=" << hdfs_table_->id() 
 988                             << " partition_id=" << partition_id 
 989                             << "\n" << PrintThrift(runtime_state_->fragment_params()); 
 990   ScannerContext context(runtime_state_, this, partition, scan_range); 
 991   scoped_ptr<HdfsScanner> scanner; 
 992   Status status = CreateAndPrepareScanner(partition, &context, &scanner); 
 993   if (!status.ok()) { 
 994     // If preparation fails, avoid leaking unread buffers in the scan_range. 
 995     scan_range->Cancel(status); 
 996  
 997     if (VLOG_QUERY_IS_ON) { 
 998       stringstream ss; 
 999       ss << "Error preparing scanner for scan range " << scan_range->file() << 
1000           "(" << scan_range->offset() << ":" << scan_range->len() << ")."; 
1001       ss << endl << runtime_state_->ErrorLog(); 
1002       VLOG_QUERY << ss.str(); 
1003     } 
1004     return status; 
1005   } 
1006   // If the parquet table is being handled, 
1007   // IMPALA-1661: 
1008   if (partition->file_format() == THdfsFileFormat::PARQUET) { 
1009         const Bitmap* bitmap_filter = 0; 
1010         bool conjuncts_passed = true; 
1011         uint32_t fragment_seed = -1; 
1012         const vector<ExprContext*>& value_ctxs = partition->partition_key_value_ctxs(); 
1013         int tuple_offset = -1; 
1014         uint32_t filters_size = 0; 
1015         uint32_t tuple_size = tuple_desc_->slots().size(); 
1016         uint32_t partition_key_size = partition_key_slots_.size(); 
1017         Tuple* tuple = InitEmptyTemplateTuple(*tuple_desc_); 
1018         // If tuple_desc_ contains more slots than partition_key_slots_, 
1019         // the 'else' block should be passed. 
1020         // TODO:The sentence 'if (tuple_desc_->slots().size() > num_partition_keys()) {' 
1021         // can work well? 
1022         if (tuple_desc_->slots().size() <= partition_key_slots_.size()) { 
1023           filters_size = runtime_state_->slot_bitmap_filters_size(); 
1024           if (filters_size) { 
1025             { 
1026             unique_lock<mutex> l(lock_); 
1027             for (int i = 0; i < partition_key_slots_.size(); ++i) { 
1028               const SlotDescriptor* slot_desc = partition_key_slots_[i]; 
1029               // Exprs guaranteed to be literals, so can safely be evaluated without a 
1030               // row context 
1031               void* value = value_ctxs[slot_desc->col_pos()]->GetValue(NULL); 
1032               RawValue::Write(value, tuple, slot_desc, NULL); 
1033             } 
1034             } 
1035             // Check every slot in tuple_desc_. if it's value is not in bitmap_filter, 
1036             // then conjuncts_passed is assigned 'false', or 'true'. 
1037             BOOST_FOREACH(SlotDescriptor* slot, tuple_desc_->slots()) { 
1038               if (!slot->is_materialized()) continue; 
1039               if (slot->parent() == tuple_desc_) { 
1040                 fragment_seed = runtime_state_->fragment_hash_seed(); 
1041                 tuple_offset = slot->tuple_offset(); 
1042                 bitmap_filter = runtime_state_->GetBitmapFilter(slot->id()); 
1043                 if ((bitmap_filter != NULL)) { 
1044                   uint32_t h = RawValue::GetHashValue(tuple->GetSlot(slot->tuple_offset()), 
1045                                                       slot->type(), fragment_seed); 
1046                   conjuncts_passed = bitmap_filter->Get<true>(h); 
1047                   // if conjuncts_passed is false, it means this scanner should be skipped. 
1048                   if (!conjuncts_passed) { 
1048                   if (!conjuncts_passed) {
1049                     //unique_lock<mutex> l(lock_);
1050                     //status_ = Status::CANCELLED;
1051                     //status_ = Status::OK();
1052                     //SetDone();
1053                     scan_range->Cancel(Status::CANCELLED);
1054                     scanner->Close();
1055                     //return Status::OK();
1056                     //return Status("Skip this scan_range : ");
1057                     return Status::OK();
1058                   }
1059                 }
1060                 conjuncts_passed = true;
1061               }
1062             } // end BOOST_FOREACH(SlotDescriptor* slot, tuple_desc_->slot())
1063           } // end if (filters_size)
1064         } // end if (tuple_desc_->slots().size() <= partition_key_slots_.size())
1065   }
1066   status = scanner->ProcessSplit();
1067   if (VLOG_QUERY_IS_ON && !status.ok() && !runtime_state_->error_log().empty()) {
1068     // This thread hit an error, record it and bail
1069     stringstream ss;
1070     ss << "Scan node (id=" << id() << ") ran into a parse error for scan range "
1071        << scan_range->file() << "(" << scan_range->offset() << ":"
1072        << scan_range->len() << ").";
1073     if (partition->file_format() != THdfsFileFormat::PARQUET) {
1074       // Parquet doesn't read the range end to end so the current offset isn't useful.
1075       // TODO: make sure the parquet reader is outputting as much diagnostic
1076       // information as possible.
1077       ScannerContext::Stream* stream = context.GetStream();
1078       ss << " Processed " << stream->total_bytes_returned() << " bytes.";
1079     }
1080     ss << endl << runtime_state_->ErrorLog();
1081     VLOG_QUERY << ss.str();
1082   }
1083 
1084   scanner->Close();
1085   return status;
1086 }
