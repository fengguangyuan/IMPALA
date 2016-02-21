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
