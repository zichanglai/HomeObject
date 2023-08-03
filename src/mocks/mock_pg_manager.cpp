#include "mock_homeobject.hpp"

#include <boost/uuid/uuid_io.hpp>

#define WITH_PG_LOCK(e)                                                                                                \
    auto err = (e);                                                                                                    \
    {                                                                                                                  \
        auto lg = std::scoped_lock(_pg_lock);

#define CB_FROM_DATA_LOCKS                                                                                             \
    }                                                                                                                  \
    if (cb) cb(err);

namespace homeobject {

void MockHomeObject::create_pg(PGInfo const& pg_info, PGManager::ok_cb const& cb) {
    LOGINFO("Creating PG: [{}] of [{}] members", pg_info.id, pg_info.members.size());
    if (std::none_of(pg_info.members.begin(), pg_info.members.end(),
                     [](PGMember const& m) { return 0 < m.priority; })) {
        LOGERROR("No possible leader for PG: [{}]", pg_info.id);
        if (cb) cb(PGError::INVALID_ARG);
        return;
    }
    WITH_PG_LOCK(PGError::INVALID_ARG)
    if (auto [_, happened] = _pg_map.try_emplace(pg_info.id, pg_info, std::unordered_set< shard_id >()); happened)
        err = PGError::OK;
    else
        LOGWARN("PG already exists [{}]!", pg_info.id);
    CB_FROM_DATA_LOCKS
}

void MockHomeObject::replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member,
                                    PGManager::ok_cb const& cb) {
    LOGINFO("Replacing PG: [{}] member [{}] with [{}]", id, to_string(old_member), to_string(new_member.id));
    if (old_member == new_member.id) {
        LOGWARN("Rejecting replace_member with identical replacement SvcId [{}]!", to_string(old_member));
        if (cb) cb(PGError::INVALID_ARG);
        return;
    }
    WITH_PG_LOCK(PGError::UNKNOWN_PG)
    if (auto pg_it = _pg_map.find(id); _pg_map.end() != pg_it) {
        if (auto& members = pg_it->second.first.members; 0 < members.erase(old_member)) {
            err = members.insert(new_member).second ? PGError::OK : PGError::INVALID_ARG;
            if (PGError::OK != err) LOGERROR("Already have this member [{}] in [pg={}]", to_string(new_member.id), id);
        } else
            err = PGError::UNKNOWN_PEER;
    }
    CB_FROM_DATA_LOCKS
}

extern std::shared_ptr< HomeObject > init_homeobject(init_params const& params) {
    return std::make_shared< MockHomeObject >(params.lookup);
}

} // namespace homeobject
