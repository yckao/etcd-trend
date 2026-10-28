package replay

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"github.com/yckao/etcd-trend/third_party/etcd-server/etcdserver/api/membership"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/membershippb"
)

var legacyMemberPath = regexp.MustCompile(`^/0/members/([0-9a-fA-F]+)/attributes$`)

// decodeRequest follows etcd's legacy internal metadata conversion. Older
// members encode version/member attributes as v2 Requests, either directly or
// inside InternalRaftRequest.V2. Genuine etcd v2 keyspace writes are not part of
// Kubernetes v3 storage and remain explicitly unsupported.
func decodeRequest(data []byte) (pb.InternalRaftRequest, error) {
	var req pb.InternalRaftRequest
	if err := req.Unmarshal(data); err == nil {
		if req.V2 != nil {
			return adaptLegacyMetadata(req.V2)
		}
		if requestType(&req) != "unknown" {
			return req, nil
		}
	}
	var legacy pb.Request
	if err := legacy.Unmarshal(data); err != nil {
		return req, fmt.Errorf("invalid internal Raft request: %w", err)
	}
	return adaptLegacyMetadata(&legacy)
}

func adaptLegacyMetadata(r *pb.Request) (pb.InternalRaftRequest, error) {
	var req pb.InternalRaftRequest
	if r.Method != "PUT" {
		return req, fmt.Errorf("unsupported legacy v2 request method %q", r.Method)
	}
	req.Header = &pb.RequestHeader{ID: r.ID}
	if r.Path == membership.StoreClusterVersionKey() {
		req.ClusterVersionSet = &membershippb.ClusterVersionSetRequest{Ver: r.Val}
		return req, nil
	}
	if m := legacyMemberPath.FindStringSubmatch(r.Path); m != nil {
		id, err := strconv.ParseUint(m[1], 16, 64)
		if err != nil {
			return req, fmt.Errorf("invalid legacy member ID: %w", err)
		}
		var attrs membership.Attributes
		if err = json.Unmarshal([]byte(r.Val), &attrs); err != nil {
			return req, fmt.Errorf("invalid legacy member attributes: %w", err)
		}
		req.ClusterMemberAttrSet = &membershippb.ClusterMemberAttrSetRequest{Member_ID: id, MemberAttributes: &membershippb.Attributes{Name: attrs.Name, ClientUrls: attrs.ClientURLs}}
		return req, nil
	}
	return req, fmt.Errorf("unsupported legacy v2 keyspace request; only internal cluster/member metadata is supported")
}
