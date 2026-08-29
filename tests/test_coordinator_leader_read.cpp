// What a read of the leader key means — and specifically, that "there is no leader" and "I could not
// find out" are different answers.
//
// `get_cluster_state()` returned std::nullopt for four unrelated reasons: not connected, an empty
// HTTP response, a key that genuinely was not there, and a body that would not parse. A primary that
// read that as "the key is gone" would step down on every transient etcd error, so it read it as "no
// information" — and therefore could not react to the key actually disappearing. That is the cause of
// roadmap #82's window, in which two nodes both hold the role and both accept writes.
//
// These tests exist without an etcd on purpose. The etcd-backed suite is opt-in and is not
// registered with ctest, so a regression guard living there would never run.

#include "orderbook/coordinator.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

using LeaderRead = ob::CoordinatorClient::LeaderRead;

/// A range response the way etcd sends one, with the value base64-encoded.
std::string range_response_with(const std::string& value_json) {
    // The encoder lives in the implementation, so this mirrors it: the tests care about the shape of
    // the envelope, and a wrong encoding would show up as Unavailable rather than as a false pass.
    static const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string b64;
    size_t i = 0;
    while (i + 2 < value_json.size()) {
        uint32_t n = (static_cast<uint32_t>(static_cast<uint8_t>(value_json[i])) << 16) |
                     (static_cast<uint32_t>(static_cast<uint8_t>(value_json[i + 1])) << 8) |
                     static_cast<uint32_t>(static_cast<uint8_t>(value_json[i + 2]));
        b64 += tbl[(n >> 18) & 63];
        b64 += tbl[(n >> 12) & 63];
        b64 += tbl[(n >> 6) & 63];
        b64 += tbl[n & 63];
        i += 3;
    }
    if (i + 1 == value_json.size()) {
        uint32_t n = static_cast<uint32_t>(static_cast<uint8_t>(value_json[i])) << 16;
        b64 += tbl[(n >> 18) & 63];
        b64 += tbl[(n >> 12) & 63];
        b64 += "==";
    } else if (i + 2 == value_json.size()) {
        uint32_t n = (static_cast<uint32_t>(static_cast<uint8_t>(value_json[i])) << 16) |
                     (static_cast<uint32_t>(static_cast<uint8_t>(value_json[i + 1])) << 8);
        b64 += tbl[(n >> 18) & 63];
        b64 += tbl[(n >> 12) & 63];
        b64 += tbl[(n >> 6) & 63];
        b64 += '=';
    }
    return "{\"header\":{\"revision\":\"7\"},\"kvs\":[{\"key\":\"L29iL2xlYWRlcg==\",\"value\":\"" +
           b64 + "\"}],\"count\":\"1\"}";
}

}  // namespace

TEST(LeaderRead, APresentKeyIsPresentAndItsContentsArriveWithIt) {
    ob::ClusterState written;
    written.leader_node_id = "node-2";
    written.leader_address = "127.0.0.1:5556";
    written.epoch          = ob::EpochValue{41};
    written.lease_id       = 7654321;

    ob::ClusterState out;
    const auto verdict = ob::CoordinatorClient::interpret_leader_response(
        range_response_with(written.to_json()), out);

    ASSERT_EQ(verdict, LeaderRead::Present);
    EXPECT_EQ(out.leader_node_id, "node-2");
    EXPECT_EQ(out.leader_address, "127.0.0.1:5556");
    EXPECT_EQ(out.epoch.term, 41u);
}

TEST(LeaderRead, ASuccessfulRangeOverAMissingKeyIsAbsentNotUnavailable) {
    // This is the case the whole change is for. etcd answers a range over a key that is not there
    // with a body carrying no `kvs` at all — a successful read whose answer is "nobody holds it".
    ob::ClusterState out;
    EXPECT_EQ(ob::CoordinatorClient::interpret_leader_response(
                  "{\"header\":{\"revision\":\"9\"}}", out),
              LeaderRead::Absent);

    // And with an explicit zero count, which is the other shape etcd uses.
    EXPECT_EQ(ob::CoordinatorClient::interpret_leader_response(
                  "{\"header\":{\"revision\":\"9\"},\"count\":\"0\"}", out),
              LeaderRead::Absent);
}

TEST(LeaderRead, AnEmptyResponseIsUnavailable) {
    // http_post() returns an empty string for a transport failure and for any status >= 400, so
    // there is nothing here to read as an answer about the key.
    ob::ClusterState out;
    EXPECT_EQ(ob::CoordinatorClient::interpret_leader_response("", out), LeaderRead::Unavailable);
}

TEST(LeaderRead, AKeyWhoseBodyWillNotParseIsUnavailableNotAbsent) {
    // The key exists. Calling that Absent would let a candidate claim a role somebody else holds,
    // which is worse than waiting for a readable answer.
    ob::ClusterState out;
    EXPECT_EQ(ob::CoordinatorClient::interpret_leader_response(
                  range_response_with("{ this is not json"), out),
              LeaderRead::Unavailable);

    EXPECT_EQ(ob::CoordinatorClient::interpret_leader_response(
                  range_response_with(""), out),
              LeaderRead::Absent)
        << "an empty value is indistinguishable from no value in this envelope, and Absent is the "
           "safe reading of it: a candidate then waits out the lease rather than claiming at once";
}

TEST(LeaderRead, TheThreeAnswersAreDistinct) {
    // The regression this guards is collapse, not any single case: if two of the three ever become
    // the same value again, the primary loses the ability to tell "gone" from "unknown".
    ob::ClusterState out;
    const auto present = ob::CoordinatorClient::interpret_leader_response(
        range_response_with(ob::ClusterState{"node-1", "127.0.0.1:1", ob::EpochValue{1}, 2}.to_json()), out);
    const auto absent = ob::CoordinatorClient::interpret_leader_response("{\"header\":{}}", out);
    const auto unavailable = ob::CoordinatorClient::interpret_leader_response("", out);

    EXPECT_NE(present, absent);
    EXPECT_NE(present, unavailable);
    EXPECT_NE(absent, unavailable);
}
