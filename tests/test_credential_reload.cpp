// The client secrets and permissions read again without a restart (#258).
//
// A reload replaces the store in force whole, or leaves it as it was: these tests hold the second
// half for every way the start refuses a file - the mode, a short secret, a duplicate, an empty file,
// a client secret that is also the cluster's, the permissions file, the migration identity - and the
// one way only a reload refuses one, the migration identity's secret changed. What a client loop does
// with a reload - the sessions of the identities it took away or changed closed, the others kept - is
// held on a live node, in `tests/integration/test_credential_reload.py`.

#include "orderbook/auth.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/tcp_server.hpp"

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr const char* kA  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr const char* kA2 = "a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2";
constexpr const char* kB  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr const char* kC  = "cccccccccccccccccccccccccccccccccccccccccccc";
constexpr const char* kCluster = "dddddddddddddddddddddddddddddddddddddddddddd";

/// A directory of its own, with a clients file, a permissions file and a cluster file in it.
struct Files {
    fs::path dir;
    std::string clients;
    std::string acl;
    std::string cluster;

    Files() {
        static std::atomic<int> counter{0};
        dir = fs::temp_directory_path() /
              ("ob_credreload_" + std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1)));
        fs::create_directories(dir);
        clients = (dir / "clients").string();
        acl     = (dir / "permissions").string();
        cluster = (dir / "cluster").string();
        write(cluster, std::string(kCluster) + "\n", 0600);
    }
    ~Files() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    static void write(const std::string& path, const std::string& contents, mode_t mode) {
        {
            std::ofstream out(path, std::ios::binary | std::ios::trunc);
            out << contents;
        }
        ::chmod(path.c_str(), mode);
    }
};

ob::ServerConfig config_of(const Files& f, bool acl = false, const std::string& migration = "") {
    ob::ServerConfig c;
    c.auth_secret_file    = f.clients;
    c.cluster_secret_file = f.cluster;
    if (acl) c.auth_acl_file = f.acl;
    c.migration_identity = migration;
    return c;
}

ob::SecretStore cluster_of(const Files& f) { return ob::SecretStore::load_cluster_file(f.cluster); }

}  // namespace

TEST(CredentialReload, PermissionsAreEqualWithTheSameRoleAndTheSameLists) {
    ob::Permissions a{ob::Role::Write, {"BTC*"}, {"BINANCE"}};
    ob::Permissions b = a;
    EXPECT_TRUE(a == b);
    b.role = ob::Role::Read;
    EXPECT_FALSE(a == b) << "another role";
    b = a;
    b.symbols.push_back("ETH*");
    EXPECT_FALSE(a == b) << "another list of symbols";
    b = a;
    b.exchanges.clear();
    EXPECT_FALSE(a == b) << "another list of exchanges";
}

TEST(CredentialReload, AFileThatLoadsIsInForceAndItsGenerationMoves) {
    Files f;
    Files::write(f.clients, std::string("alice ") + kA + "\nbob " + kB + "\n", 0600);
    const ob::ServerConfig config = config_of(f);
    const ob::SecretStore cluster = cluster_of(f);
    ob::ClientCredentials credentials(ob::load_client_secrets(config, cluster));
    EXPECT_EQ(credentials.generation(), 0u);
    EXPECT_EQ(credentials.current().store->identities(), (std::vector<std::string>{"alice", "bob"}));

    Files::write(f.clients, std::string("alice ") + kA2 + "\ncarol " + kC + "\n", 0600);
    ob::MetricsRegistry registry;
    EXPECT_EQ(ob::reload_client_credentials(config, cluster, credentials, &registry),
              ob::CredentialReload::Reloaded);
    const auto now = credentials.current();
    EXPECT_EQ(now.generation, 1u);
    EXPECT_EQ(now.store->identities(), (std::vector<std::string>{"alice", "carol"}));
    EXPECT_EQ(now.store->find("alice")->secret, kA2);
    EXPECT_EQ(now.store->find("bob"), nullptr);
    EXPECT_EQ(registry.counter_value("ob_auth_reloads_total"), 1u);
    EXPECT_EQ(registry.counter_value("ob_auth_reload_failures_total"), 0u);
}

TEST(CredentialReload, EveryFileTheStartRefusesLeavesTheCredentialsInForce) {
    Files f;
    Files::write(f.clients, std::string("alice ") + kA + "\nbob " + kB + "\n", 0600);
    Files::write(f.acl, "alice role=admin\nbob role=read\n", 0644);
    const ob::ServerConfig config = config_of(f, /*acl=*/true);
    const ob::SecretStore cluster = cluster_of(f);
    ob::ClientCredentials credentials(ob::load_client_secrets(config, cluster));
    const auto in_force = credentials.current().store;

    struct Case {
        const char* what;
        std::string clients;
        mode_t mode;
        std::string acl;
    };
    const std::string good = std::string("alice ") + kA + "\nbob " + kB + "\n";
    const std::vector<Case> cases = {
        {"a file others may read", good, 0644, "alice role=admin\nbob role=read\n"},
        {"a secret shorter than 32 characters", "alice short\nbob " + std::string(kB) + "\n", 0600,
         "alice role=admin\nbob role=read\n"},
        {"an identity twice", good + "alice " + kC + "\n", 0600, "alice role=admin\nbob role=read\n"},
        {"no credential at all", "# nobody\n", 0600, "alice role=admin\nbob role=read\n"},
        {"a client secret that is the cluster's", std::string("alice ") + kCluster + "\nbob " + kB + "\n",
         0600, "alice role=admin\nbob role=read\n"},
        {"a permissions file missing an identity", good, 0600, "alice role=admin\n"},
        {"a permissions file with a role nobody has", good, 0600, "alice role=root\nbob role=read\n"},
    };
    ob::MetricsRegistry registry;
    for (const Case& c : cases) {
        Files::write(f.clients, c.clients, c.mode);
        Files::write(f.acl, c.acl, 0644);
        EXPECT_EQ(ob::reload_client_credentials(config, cluster, credentials, &registry),
                  ob::CredentialReload::Refused)
            << c.what;
        const auto now = credentials.current();
        EXPECT_EQ(now.store, in_force) << c.what << ": the store in force was replaced";
        EXPECT_EQ(now.generation, 0u) << c.what;
    }
    EXPECT_EQ(registry.counter_value("ob_auth_reload_failures_total"), cases.size());
    EXPECT_EQ(registry.counter_value("ob_auth_reloads_total"), 0u);

    // The control: the good files again are taken.
    Files::write(f.clients, good, 0600);
    Files::write(f.acl, "alice role=admin\nbob role=write\n", 0644);
    EXPECT_EQ(ob::reload_client_credentials(config, cluster, credentials, &registry),
              ob::CredentialReload::Reloaded);
    EXPECT_EQ(credentials.current().store->find("bob")->permissions.role, ob::Role::Write);
}

// The shard coordinator moves symbols with the migration identity's secret it took at the start, so
// a reload that changes that secret is refused; one that leaves it and changes the others is not.
TEST(CredentialReload, TheMigrationIdentitysSecretIsTheOneTheNodeStartedWith) {
    Files f;
    Files::write(f.clients, std::string("mover ") + kA + "\nbob " + kB + "\n", 0600);
    const ob::ServerConfig config = config_of(f, /*acl=*/false, "mover");
    const ob::SecretStore cluster = cluster_of(f);
    ob::ClientCredentials credentials(ob::load_client_secrets(config, cluster));

    Files::write(f.clients, std::string("mover ") + kA2 + "\nbob " + kB + "\n", 0600);
    EXPECT_EQ(ob::reload_client_credentials(config, cluster, credentials, nullptr),
              ob::CredentialReload::Refused);
    Files::write(f.clients, std::string("bob ") + kB + "\n", 0600);
    EXPECT_EQ(ob::reload_client_credentials(config, cluster, credentials, nullptr),
              ob::CredentialReload::Refused)
        << "the migration identity taken away";
    EXPECT_EQ(credentials.generation(), 0u);

    Files::write(f.clients, std::string("mover ") + kA + "\nbob " + kC + "\n", 0600);
    EXPECT_EQ(ob::reload_client_credentials(config, cluster, credentials, nullptr),
              ob::CredentialReload::Reloaded);
    EXPECT_EQ(credentials.current().store->find("bob")->secret, kC);
}

TEST(CredentialReload, ANodeWithoutClientAuthenticationHasNothingToReload) {
    Files f;
    ob::ServerConfig config;   // no --auth-secret-file
    const ob::SecretStore cluster = cluster_of(f);
    ob::ClientCredentials credentials(ob::load_client_secrets(config, cluster));
    ASSERT_TRUE(credentials.current().store->empty());
    ob::MetricsRegistry registry;
    EXPECT_EQ(ob::reload_client_credentials(config, cluster, credentials, &registry),
              ob::CredentialReload::NothingToReload);
    EXPECT_EQ(credentials.generation(), 0u);
    EXPECT_EQ(registry.counter_value("ob_auth_reloads_total"), 0u);
    EXPECT_EQ(registry.counter_value("ob_auth_reload_failures_total"), 0u);
}
