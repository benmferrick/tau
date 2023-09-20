package libdream

import (
	"context"
	"fmt"
	"sync"

	peercore "github.com/libp2p/go-libp2p/core/peer"
	commonIface "github.com/taubyte/go-interfaces/common"
	"github.com/taubyte/go-interfaces/services/auth"
	"github.com/taubyte/go-interfaces/services/hoarder"
	"github.com/taubyte/go-interfaces/services/monkey"
	"github.com/taubyte/go-interfaces/services/patrick"
	"github.com/taubyte/go-interfaces/services/seer"
	"github.com/taubyte/go-interfaces/services/substrate"
	"github.com/taubyte/go-interfaces/services/tns"
	commonSpecs "github.com/taubyte/go-specs/common"
	peer "github.com/taubyte/p2p/peer"
)

//this method returns the "name" of a Universe type, which is a string
func (u *Universe) Name() string {
	return u.name
}

//this method returns "all" for a Universe type, which is a []peer.Node
func (u *Universe) All() []peer.Node {
	return u.all
}

//this method will take an inputted string "id", returns "nil" if not there, returns node that matches the "id" if present
func (u *Universe) Lookup(id string) (*NodeInfo, bool) {
	u.lock.RLock()
	node, exist := u.lookups[id]
	u.lock.RUnlock()
	if !exist { //checks if the node matching "id" exists in this Universe
		return nil, false
	}
	return node, true
}

//this method return string "root" for Universe type
func (u *Universe) Root() string {
	return u.root
}

//this method return context "Context" for Universe type
func (u *Universe) Context() context.Context {
	return u.ctx
}

//this method "meshes" or combines a new node(s) to existing in a Universe type, no return
func (u *Universe) Mesh(newNodes ...peer.Node) {
	ctx, ctxC := context.WithTimeout(u.ctx, MeshTimeout)
	defer ctxC()

	u.lock.RLock()
	var wg sync.WaitGroup
	for _, n0 := range newNodes {
		for _, n1 := range u.all {
			if n0 != n1 {
				wg.Add(1)
				go func(n0, n1 peer.Node) {
					defer wg.Done()
					n0.Peer().Connect(
						ctx,
						peercore.AddrInfo{
							ID:    n1.ID(),
							Addrs: n1.Peer().Addrs(),
						},
					)
				}(n0, n1)
			}
		}
	}
	wg.Wait()
	u.lock.RUnlock()

	u.lock.Lock()
	u.all = append(u.all, newNodes...)
	u.lock.Unlock()
}

//this method returns seer.Service() type of a Universe type if exists, "nil" otherwise
func (u *Universe) Seer() seer.Service {
	//this both sets "ok" to the presense or lack of seer.Service and ret to the actual type to return
	ret, ok := first[seer.Service](u, u.service["seer"].nodes)
	if !ok { //checks if exists
		return nil
	}
	return ret
}

//returns string "pid" for a seer.Service type of a Universe type
func (u *Universe) SeerByPid(pid string) (seer.Service, bool) {
	return byId[seer.Service](u, u.service["seer"].nodes, pid)
}

func (u *Universe) Auth() auth.Service {
	ret, ok := first[auth.Service](u, u.service["auth"].nodes)
	if !ok {
		return nil
	}
	return ret
}

func (u *Universe) AuthByPid(pid string) (auth.Service, bool) {
	return byId[auth.Service](u, u.service["auth"].nodes, pid)
}

func (u *Universe) Patrick() patrick.Service {
	ret, ok := first[patrick.Service](u, u.service["patrick"].nodes)
	if !ok {
		return nil
	}
	return ret
}

func (u *Universe) PatrickByPid(pid string) (patrick.Service, bool) {
	return byId[patrick.Service](u, u.service["patrick"].nodes, pid)
}

func (u *Universe) TNS() tns.Service {
	ret, ok := first[tns.Service](u, u.service["tns"].nodes)
	if !ok {
		return nil
	}
	return ret
}

func (u *Universe) TnsByPid(pid string) (tns.Service, bool) {
	return byId[tns.Service](u, u.service["tns"].nodes, pid)
}

func (u *Universe) Monkey() monkey.Service {
	ret, ok := first[monkey.Service](u, u.service["monkey"].nodes)
	if !ok {
		return nil
	}
	return ret
}

func (u *Universe) MonkeyByPid(pid string) (monkey.Service, bool) {
	return byId[monkey.Service](u, u.service["monkey"].nodes, pid)
}

func (u *Universe) Hoarder() hoarder.Service {
	ret, ok := first[hoarder.Service](u, u.service["hoarder"].nodes)
	if !ok {
		return nil
	}
	return ret
}

func (u *Universe) HoarderByPid(pid string) (hoarder.Service, bool) {
	return byId[hoarder.Service](u, u.service["hoarder"].nodes, pid)
}

func (u *Universe) Substrate() substrate.Service {
	ret, ok := first[substrate.Service](u, u.service["substrate"].nodes)
	if !ok {
		return nil
	}
	return ret
}

func (u *Universe) SubstrateByPid(pid string) (substrate.Service, bool) {
	return byId[substrate.Service](u, u.service["substrate"].nodes, pid)
}

func byId[T any](u *Universe, i map[string]commonIface.Service, pid string) (T, bool) {
	var result T
	u.lock.RLock()
	defer u.lock.RUnlock()
	a, ok := i[pid]
	if !ok {
		return result, false
	}
	_a, ok := a.(T)
	return _a, ok
}

func first[T any](u *Universe, i map[string]commonIface.Service) (T, bool) {
	var _nil T
	u.lock.RLock()
	defer u.lock.RUnlock()
	for _, s := range i {
		_s, ok := s.(T)
		if !ok || s == nil {
			return _nil, false
		}
		return _s, true
	}
	return _nil, false
}

func (u *Universe) ListNumber(name string) int {
	return len(u.service[name].nodes)
}

//this method returns errors 
func (u *Universe) Kill(name string) error {
	var isService bool
	for _, service := range commonSpecs.Protocols { //checks in input "name" matches any in the commonSpecs.Protocols list
		if name == service { //if there is a match "isService" set to true
			isService = true
			break
		}
	}

	if isService { //if match was found above
		ids, err := u.GetServicePids(name) //gets service pid based on name, and checks if exist
		if err != nil { //return the error if exists
			return err
		}
		if len(ids) == 0 { //if error does not exist, return killing failed due to not existing
			return fmt.Errorf("killing %s failed with: does not exist", name)
		}

		return u.killServiceByNameId(name, ids[0])

	} else { //in case of match not being found above
		u.lock.RLock()
		simple, exist := u.simples[name] //checks if this "name" exists in the "simples" array for this universe
		u.lock.RUnlock()
		if !exist { //if it does not exists, return failed
			return fmt.Errorf("killing %s failed with: does not exist", name)
		}

		//returns the error from the simple array if exists
		return u.killSimpleByNameId(name, simple.ID().Pretty())
	}
}
