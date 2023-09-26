#include "hs_homeobject.hpp"
#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "lib/homeobject_impl.hpp"

SISL_LOGGING_DECL(blobmgr)

namespace homeobject {

BlobManager::AsyncResult< blob_id_t > HSHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
    auto& pg_id = shard.placement_group;
    shared< homestore::ReplDev > repl_dev;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        if (iter == _pg_map.end()) {
            LOGERROR("failed to put blob with non-exist pg [{}]", pg_id);
            return folly::makeUnexpected(BlobError::UNKNOWN_PG);
        }
        repl_dev = static_cast< HS_PG* >(iter->second.get())->repl_dev_;
    }

    RELEASE_ASSERT(repl_dev != nullptr, "Repl dev instance null");

    const uint32_t needed_size = sizeof(ReplicationMessageHeader);
    auto req = repl_result_ctx< BlobManager::Result< BlobInfo > >::make(needed_size, 512);

    uint8_t* raw_ptr = req->hdr_buf_.bytes;
    ReplicationMessageHeader* header = new (raw_ptr) ReplicationMessageHeader();
    header->msg_type = ReplicationMessageType::PUT_BLOB_MSG;
    header->payload_size = 0;
    header->payload_crc = 0;
    header->shard_id = shard.id;
    header->pg_id = pg_id;
    header->header_crc = header->calculate_crc();

    BlobHeader blob_header{};
    blob_header.shard_id = shard.id;
    blob_header.total_size = blob.body.size;
    blob_header.hash_algorithm = BlobHeader::HashAlgorithm::CRC32;
    blob_header.compute_blob_hash(blob.body, false /* verify */);

    sisl::sg_list sgs;
    sgs.size = 0;
    auto block_size = repl_dev->get_blk_size();
    auto size = sisl::round_up(sizeof(blob_header), block_size);
    auto buf_header = iomanager.iobuf_alloc(block_size, size);
    std::memcpy(buf_header, &blob_header, sizeof(blob_header));

    // Create blob header.
    sgs.iovs.emplace_back(iovec{.iov_base = buf_header, .iov_len = size});
    sgs.size += size;

    // Append blob bytes.
    sgs.iovs.emplace_back(iovec{.iov_base = blob.body.bytes, .iov_len = blob.body.size});
    sgs.size += blob.body.size;

    // Append metadata and update the offsets and total size.
    if (!blob.user_key.empty()) {
        size_t user_key_size = blob.user_key.size();
        sgs.iovs.emplace_back(iovec{.iov_base = blob.user_key.data(), .iov_len = user_key_size});
        sgs.size += user_key_size;
        blob_header.total_size += user_key_size;
        blob_header.meta_data_offset = blob.body.size;
    }

    repl_dev->async_alloc_write(req->hdr_buf_, sisl::blob{}, sgs, req);
    return req->result().deferValue([this, header, buf_header, blob = std::move(blob)](
                                        const auto& result) -> BlobManager::AsyncResult< blob_id_t > {
        header->~ReplicationMessageHeader();
        iomanager.iobuf_free(buf_header);

        if (result.hasError()) { return folly::makeUnexpected(result.error()); }
        auto blob_info = result.value();
        LOGTRACEMOD(blobmgr, "Put blob success shard {} blob {} pbas {}", blob_info.shard_id, blob_info.blob_id,
                    blob_info.pbas.to_string());

        return blob_info.blob_id;
    });
}

void HSHomeObject::on_blob_put_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                      const homestore::MultiBlkId& pbas,
                                      cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< BlobManager::Result< BlobInfo > >* ctx{nullptr};
    if (hs_ctx != nullptr) {
        ctx = boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< BlobInfo > > >(hs_ctx).get();
    }

    auto msg_header = r_cast< ReplicationMessageHeader* >(header.bytes);
    if (msg_header->corrupted()) {
        LOGERROR("replication message header is corrupted with crc error, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH)); }
        return;
    }

    shared< BlobIndexTable > index_table;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(msg_header->pg_id);
        if (iter == _pg_map.end()) {
            LOGERROR("Couldnt find pg {} for blob {}", msg_header->pg_id, lsn);
            ctx->promise_.setValue(folly::makeUnexpected(BlobError::UNKNOWN_PG));
            return;
        }

        index_table = static_cast< HS_PG* >(iter->second.get())->index_table_;
        RELEASE_ASSERT(index_table != nullptr, "Index table not intialized");
    }

    BlobInfo blob_info;
    blob_info.shard_id = msg_header->shard_id;
    blob_info.blob_id = lsn;
    blob_info.pbas = pbas;

    // Write to index table with key {shard id, blob id } and value {pba}.
    auto r = add_to_index_table(index_table, blob_info);
    if (r.hasError()) {
        LOGERROR("Failed to insert into index table for blob {} err {}", lsn, r.error());
        ctx->promise_.setValue(folly::makeUnexpected(r.error()));
        return;
    }

    if (ctx) { ctx->promise_.setValue(BlobManager::Result< BlobInfo >(blob_info)); }
}

BlobManager::AsyncResult< Blob > HSHomeObject::_get_blob(ShardInfo const& shard, blob_id_t blob_id, uint64_t req_offset,
                                                         uint64_t req_len) const {
    auto& pg_id = shard.placement_group;
    shared< BlobIndexTable > index_table;
    shared< homestore::ReplDev > repl_dev;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        if (iter == _pg_map.end()) {
            LOGERROR("failed to do get blob with non-exist pg [{}]", pg_id);
            return folly::makeUnexpected(BlobError::UNKNOWN_PG);
        }

        repl_dev = static_cast< HS_PG* >(iter->second.get())->repl_dev_;
        index_table = static_cast< HS_PG* >(iter->second.get())->index_table_;
    }

    RELEASE_ASSERT(repl_dev != nullptr, "Repl dev instance null");
    RELEASE_ASSERT(index_table != nullptr, "Index table instance null");

    auto r = get_from_index_table(index_table, shard.id, blob_id);
    if (!r) {
        LOGWARN("Blob not found in index id {} shard {}", blob_id, shard.id);
        return folly::makeUnexpected(r.error());
    }

    auto multi_blkids = r.value();
    auto block_size = repl_dev->get_blk_size();
    auto sgs_ptr = std::make_shared< sisl::sg_list >();
    auto total_size = multi_blkids.blk_count() * block_size;
    auto iov_base = iomanager.iobuf_alloc(block_size, total_size);
    sgs_ptr->size = total_size;
    sgs_ptr->iovs.emplace_back(iovec{.iov_base = iov_base, .iov_len = total_size});

    return repl_dev->async_read(multi_blkids, *sgs_ptr, total_size)
        .thenValue([block_size, blob_id, req_len, req_offset, shard, multi_blkids, sgs_ptr,
                    iov_base](auto&& result) mutable -> BlobManager::AsyncResult< Blob > {
            if (result) {
                LOGERROR("Failed to read blob {} shard {} err {}", blob_id, shard.id, result.value());
                iomanager.iobuf_free(iov_base);
                return folly::makeUnexpected(BlobError::READ_FAILED);
            }

            BlobHeader* header = (BlobHeader*)iov_base;
            if (!header->valid() || header->shard_id != shard.id) {
                LOGERROR("Invalid header found for blob {} shard {}", blob_id, shard.id);
                LOGERROR("Blob header {}", header->to_string());
                iomanager.iobuf_free(iov_base);
                return folly::makeUnexpected(BlobError::READ_FAILED);
            }

            auto header_size = sisl::round_up(sizeof(BlobHeader), block_size);
            size_t blob_size = header->meta_data_offset == 0 ? header->total_size : header->meta_data_offset;
            uint8_t* blob_bytes = (uint8_t*)iov_base + header_size;
            if (!header->compute_blob_hash(sisl::blob{blob_bytes, (uint32_t)blob_size}, true /* verify */)) {
                LOGERROR("Hash mismatch for blob {} shard {}", blob_id, shard.id);
                iomanager.iobuf_free(iov_base);
                return folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH);
            }

            if (req_offset + req_len > blob_size) {
                LOGERROR("Invalid offset length request in get blob {} offset {} len {} size {}", blob_id, req_offset,
                         req_len, blob_size);
                iomanager.iobuf_free(iov_base);
                return folly::makeUnexpected(BlobError::INVALID_ARG);
            }

            // Copy the blob bytes from the offset. If request len is 0, take the
            // whole blob size else copy only the request length.
            blob_bytes += req_offset;
            auto res_len = req_len == 0 ? blob_size : req_len;
            auto body = sisl::io_blob_safe(res_len);
            std::memcpy(body.bytes, blob_bytes, res_len);

            // Copy the metadata if its present.
            std::string user_key{};
            if (header->meta_data_offset != 0) {
                blob_bytes += header->meta_data_offset;
                auto meta_size = header->total_size - header->meta_data_offset;
                user_key.reserve(meta_size);
                std::memcpy(user_key.data(), blob_bytes, meta_size);
            }

            LOGTRACEMOD(blobmgr, "Blob get success for blob {} shard {} blkid {}", blob_id, shard.id,
                        multi_blkids.to_string());
            iomanager.iobuf_free(iov_base);
            return Blob(std::move(body), std::move(user_key), 0);
        });
}

homestore::blk_alloc_hints HSHomeObject::blob_put_get_blk_alloc_hints(sisl::blob const& header,
                                                                      cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< BlobManager::Result< BlobInfo > >* ctx{nullptr};
    if (hs_ctx != nullptr) {
        ctx = boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< BlobInfo > > >(hs_ctx).get();
    }

    auto msg_header = r_cast< ReplicationMessageHeader* >(header.bytes);
    if (msg_header->corrupted()) {
        LOGERROR("replication message header is corrupted with crc error shard:{}", msg_header->shard_id);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH)); }
        return {};
    }

    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(msg_header->shard_id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "Couldnt find shard id");
    auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());
    auto chunk_id = hs_shard->sb_->chunk_id;
    LOGINFO("Got shard id {} chunk id {}", msg_header->shard_id, chunk_id);
    homestore::blk_alloc_hints hints;
    hints.chunk_id_hint = chunk_id;
    return hints;
}

BlobManager::NullAsyncResult HSHomeObject::_del_blob(ShardInfo const&, blob_id_t) {
    return folly::makeUnexpected(BlobError::UNKNOWN);
}

bool HSHomeObject::BlobHeader::compute_blob_hash(const sisl::blob& blob, bool verify) {
    // If verify is false store only the computed hash in the header else do the verification also.
    bool match = false;
    if (hash_algorithm == HSHomeObject::BlobHeader::HashAlgorithm::NONE) {
        std::memset(&hash, 0, sizeof(hash));
        match = true;
    } else if (hash_algorithm == HSHomeObject::BlobHeader::HashAlgorithm::CRC32) {
        auto blob_hash = crc32_ieee(init_crc32, blob.bytes, blob.size);
        if (!verify) {
            std::memcpy(&hash, &blob_hash, sizeof(int32_t));
        } else {
            if (std::memcmp(&hash, &blob_hash, sizeof(int32_t)) == 0) {
                match = true;
            } else {
                LOGERROR("CRC32 hash mismatch computed: {} header_hash: {}",
                         hex_bytes((uint8_t*)&blob_hash, sizeof(int32_t)), hex_bytes((uint8_t*)&hash, sizeof(int32_t)));
                match = false;
            }
        }
    } else {
        RELEASE_ASSERT(false, "Hash not implemented");
    }
    return match;
}

} // namespace homeobject
