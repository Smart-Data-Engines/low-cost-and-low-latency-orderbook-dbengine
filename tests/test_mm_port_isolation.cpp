// Feature: mm-port-isolation, Property 1: Mutual Exclusivity of Replication Subsystems
// **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 4.1, 4.2**
//
// For any valid Engine configuration, if mm_config.enabled == true then
// repl_mgr_ SHALL be nullptr AND mm_mgr_ SHALL be non-null; conversely,
// if mm_config.enabled == false then mm_mgr_ SHALL be nullptr.

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include "orderbook/engine.hpp"
#include "orderbook/multi_master.hpp"
#include "orderbook/replication.hpp"

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

namespace fs = std::filesystem;

namespace {

/// RAII temporary directory for Engine data.
struct TmpDir {
    std::string path;
    TmpDir() {
        char tpl[] = "/tmp/ob_port_iso_XXXXXX";
        char* dir = ::mkdtemp(tpl);
        if (!dir) throw std::runtime_error("mkdtemp failed");
        path = dir;
    }
    ~TmpDir() { fs::remove_all(path); }
};

} // anonymous namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Property 1: Mutual Exclusivity of Replication Subsystems
//
// For any valid MultiMasterConfig + ReplicationConfig combination:
//   - If mm_config.enabled == true → multi_master_manager() != nullptr
//     AND node_role == MULTI_MASTER (implies repl_mgr_ was NOT created)
//   - If mm_config.enabled == false → multi_master_manager() == nullptr
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(MmPortIsolation,
              prop_mm_enabled_creates_only_mm_manager, ()) {
    // Generate random valid MultiMasterConfig with enabled=true
    const auto node_id = *rc::gen::inRange<uint16_t>(1, 65535);
    // Use port 0 to avoid actual port binding in tests (MM manager handles
    // bind failure gracefully). The mutual exclusivity invariant is about
    // which subsystem is *created*, not about successful port binding.

    ob::MultiMasterConfig mm_config{};
    mm_config.enabled = true;
    mm_config.node_id = node_id;
    mm_config.replication_port = 0;  // port 0 = OS-assigned (safe for tests)
    mm_config.compress = *rc::gen::arbitrary<bool>();
    mm_config.max_catchup_bytes = *rc::gen::inRange<size_t>(1024, 1024 * 1024);
    mm_config.anti_entropy_interval_sec = *rc::gen::inRange<uint32_t>(1, 300);

    // Generate random ReplicationConfig (should be ignored in MM mode).
    // Use a non-zero port to prove it's truly ignored when MM is enabled.
    const auto repl_port = *rc::gen::inRange<uint16_t>(1024, 65535);
    ob::ReplicationConfig repl_config{};
    repl_config.port = repl_port;
    repl_config.max_replicas = *rc::gen::inRange(1, 16);
    repl_config.compress = *rc::gen::arbitrary<bool>();

    TmpDir tmp;
    ob::Engine engine(tmp.path,
                      100'000'000ULL,          // flush_interval_ns
                      ob::FsyncPolicy::NONE,   // fast for tests
                      repl_config,
                      ob::ReplicationClientConfig{},
                      ob::FailoverConfig{},
                      ob::TTLConfig{},
                      mm_config);
    engine.open();

    // Assertion 1: MultiMasterManager is created
    RC_ASSERT(engine.multi_master_manager() != nullptr);

    // Assertion 2: Engine reports multi-master mode
    RC_ASSERT(engine.is_multi_master() == true);

    // Assertion 3: Node role is MULTI_MASTER (set only in MM branch,
    // confirming the else branch with ReplicationManager was NOT taken)
    RC_ASSERT(engine.node_role() == ob::NodeRole::MULTI_MASTER);

    engine.close();
}

RC_GTEST_PROP(MmPortIsolation,
              prop_mm_disabled_does_not_create_mm_manager, ()) {
    // Generate random ReplicationConfig — use port 0 (disabled) to avoid
    // bind failures. The property under test is that mm_mgr_ is nullptr
    // when MM is disabled, regardless of replication config.
    ob::ReplicationConfig repl_config{};
    repl_config.port = 0;  // disabled — avoids bind permission/conflict issues
    repl_config.max_replicas = *rc::gen::inRange(1, 16);
    repl_config.compress = *rc::gen::arbitrary<bool>();

    // MultiMasterConfig with enabled=false — generate random field values
    // to show the property holds regardless of other config fields.
    ob::MultiMasterConfig mm_config{};
    mm_config.enabled = false;
    mm_config.node_id = *rc::gen::inRange<uint16_t>(0, 65535);
    mm_config.replication_port = *rc::gen::inRange<uint16_t>(0, 65535);
    mm_config.compress = *rc::gen::arbitrary<bool>();
    mm_config.max_catchup_bytes = *rc::gen::inRange<size_t>(1024, 1024 * 1024);
    mm_config.anti_entropy_interval_sec = *rc::gen::inRange<uint32_t>(1, 300);

    TmpDir tmp;
    ob::Engine engine(tmp.path,
                      100'000'000ULL,          // flush_interval_ns
                      ob::FsyncPolicy::NONE,   // fast for tests
                      repl_config,
                      ob::ReplicationClientConfig{},
                      ob::FailoverConfig{},
                      ob::TTLConfig{},
                      mm_config);
    engine.open();

    // Assertion 1: MultiMasterManager is NOT created
    RC_ASSERT(engine.multi_master_manager() == nullptr);

    // Assertion 2: Engine does NOT report multi-master mode
    RC_ASSERT(engine.is_multi_master() == false);

    // Assertion 3: Node role is NOT MULTI_MASTER
    RC_ASSERT(engine.node_role() != ob::NodeRole::MULTI_MASTER);

    engine.close();
}
