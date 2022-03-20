package util

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-car"
	ipldprime "github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"golang.org/x/xerrors"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
)

const UnixfsLinksPerLevel = 1 << 10
const UnixfsChunkSize uint64 = 1 << 20
var logger = logging.Logger("graphsplit")
type FSBuilder struct {
	root *dag.ProtoNode
	ds   ipld.DAGService
}
func getDirKey(dirList []string, i int) (key string) {
	for j := 0; j <= i; j++ {
		key += dirList[j]
		if j < i {
			key += "."
		}
	}
	return
}
func NewFSBuilder(root *dag.ProtoNode, ds ipld.DAGService) *FSBuilder {
	return &FSBuilder{root, ds}
}

func isLinked(node *dag.ProtoNode, name string) bool {
	for _, lk := range node.Links() {
		if lk.Name == name {
			return true
		}
	}
	return false
}

type Finfo struct {
	Path      string
	Name      string
	Info      os.FileInfo
	SeekStart int64
	SeekEnd   int64
}

type fileSlice struct {
	r        *os.File
	offset   int64
	start    int64
	end      int64
	fileSize int64
}

func (f fileSlice) Read(p []byte) (n int, err error) {
	panic("implement me")
}

func GenerateCar(ctx context.Context, fileList []Finfo, parentPath string, output io.Writer, parallel int) (ipldDag string, commp string, cid string, err error) {
	bs2 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))
	dagServ := merkledag.NewDAGService(blockservice.New(bs2, offline.Exchange(bs2)))

	cidBuilder, err := merkledag.PrefixForCidVersion(1)
	if err != nil { return }
	fileNodeMap := make(map[string]*dag.ProtoNode)
	dirNodeMap := make(map[string]*dag.ProtoNode)

	var rootNode *dag.ProtoNode
	rootNode = unixfs.EmptyDirNode()
	rootNode.SetCidBuilder(cidBuilder)
	var rootKey = "root"
	dirNodeMap[rootKey] = rootNode

	fmt.Println("************ start to build ipld **************")
	// build file node
	// parallel build
	cpun := runtime.NumCPU()
	if parallel > cpun {
		parallel = cpun
	}
	pchan := make(chan struct{}, parallel)
	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	for i, item := range fileList {
		wg.Add(1)
		go func(i int, item Finfo) {
			defer func() {
				<-pchan
				wg.Done()
			}()
			pchan <- struct{}{}
			fileNode, err := BuildFileNode(item, dagServ, cidBuilder)
			if err != nil {
				logger.Warn(err)
				return
			}
			fn, ok := fileNode.(*dag.ProtoNode)
			if !ok {
				emsg := "file node should be *dag.ProtoNode"
				logger.Warn(emsg)
				return
			}
			lock.Lock()
			fileNodeMap[item.Path] = fn
			lock.Unlock()
			fmt.Println(item.Path)
			logger.Infof("file node: %s", fileNode)
		}(i, item)
	}
	wg.Wait()

	// build dir tree
	for _, item := range fileList {
		// logger.Info(item.Path)
		// logger.Infof("file name: %s, file size: %d, item size: %d, seek-start:%d, seek-end:%d", item.Name, item.Info.Size(), item.SeekEnd-item.SeekStart, item.SeekStart, item.SeekEnd)
		dirStr := path.Dir(item.Path)
		parentPath = path.Clean(parentPath)
		// when parent path equal target path, and the parent path is also a file path
		if parentPath == path.Clean(item.Path) {
			dirStr = ""
		} else if parentPath != "" && strings.HasPrefix(dirStr, parentPath) {
			dirStr = dirStr[len(parentPath):]
		}

		if strings.HasPrefix(dirStr, "/") {
			dirStr = dirStr[1:]
		}
		var dirList []string
		if dirStr == "" {
			dirList = []string{}
		} else {
			dirList = strings.Split(dirStr, "/")
		}
		fileNode, ok := fileNodeMap[item.Path]
		if !ok {
			panic("unexpected, missing file node")
		}
		if len(dirList) == 0 {
			dirNodeMap[rootKey].AddNodeLink(item.Name, fileNode)
			continue
		}
		//logger.Info(item.Path)
		//logger.Info(dirList)
		i := len(dirList) - 1
		for ; i >= 0; i-- {
			// get dirNodeMap by index
			var ok bool
			var dirNode *dag.ProtoNode
			var parentNode *dag.ProtoNode
			var parentKey string
			dir := dirList[i]
			dirKey := getDirKey(dirList, i)
			logger.Info(dirList)
			logger.Infof("dirKey: %s", dirKey)
			dirNode, ok = dirNodeMap[dirKey]
			if !ok {
				dirNode = unixfs.EmptyDirNode()
				dirNode.SetCidBuilder(cidBuilder)
				dirNodeMap[dirKey] = dirNode
			}
			// add file node to its nearest parent node
			if i == len(dirList)-1 {
				dirNode.AddNodeLink(item.Name, fileNode)
			}
			if i == 0 {
				parentKey = rootKey
			} else {
				parentKey = getDirKey(dirList, i-1)
			}
			logger.Infof("parentKey: %s", parentKey)
			parentNode, ok = dirNodeMap[parentKey]
			if !ok {
				parentNode = unixfs.EmptyDirNode()
				parentNode.SetCidBuilder(cidBuilder)
				dirNodeMap[parentKey] = parentNode
			}
			if isLinked(parentNode, dir) {
				parentNode, err = parentNode.UpdateNodeLink(dir, dirNode)
				if err != nil { return }
				dirNodeMap[parentKey] = parentNode
			} else {
				parentNode.AddNodeLink(dir, dirNode)
			}
		}
	}

	for _, node := range dirNodeMap {
		//fmt.Printf("add node to store: %v\n", node)
		//fmt.Printf("key: %s, links: %v\n", key, len(node.Links()))
		dagServ.Add(ctx, node)
	}

	rootNode = dirNodeMap[rootKey]
	fmt.Printf("root node cid: %s\n", rootNode.Cid())
	logger.Infof("start to generate car for %s", rootNode.Cid())
	genCarStartTime := time.Now()
	//car
	selector := allSelector()
	sc := car.NewSelectiveCar(ctx, bs2, []car.Dag{{Root: rootNode.Cid(), Selector: selector}})
	err = sc.Write(output)
	// cario := cario.NewCarIO()
	// err = cario.WriteCar(context.Background(), bs2, rootNode.Cid(), selector, carF)
	if err != nil { return }
	logger.Infof("generate car file completed, time elapsed: %s", time.Now().Sub(genCarStartTime))

	fsBuilder := NewFSBuilder(rootNode, dagServ)
	fsNode, err := fsBuilder.Build()
	if err != nil { return }
	fsNodeBytes, err := json.Marshal(fsNode)
	if err != nil { return }
	//logger.Info(dirNodeMap)
	fmt.Println("++++++++++++ finished to build ipld +++++++++++++")
	ipldDag = fmt.Sprintf("%s", fsNodeBytes)
	cid = rootNode.Cid().String()
	return
}

func allSelector() ipldprime.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).
		Node()
}
func BuildFileNode(item Finfo, bufDs ipld.DAGService, cidBuilder cid.Builder) (node ipld.Node, err error) {
	var r io.Reader
	f, err := os.Open(item.Path)
	if err != nil {
		return nil, err
	}
	r = f

	// read all data of item
	if item.SeekStart > 0 || item.SeekEnd > 0 {
		r = &fileSlice{
			r:        f,
			start:    item.SeekStart,
			end:      item.SeekEnd,
			fileSize: item.Info.Size(),
		}
	}

	params := ihelper.DagBuilderParams{
		Maxlinks:   UnixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: cidBuilder,
		Dagserv:    bufDs,
		NoCopy:     true,
	}
	db, err := params.New(chunker.NewSizeSplitter(r, int64(UnixfsChunkSize)))
	if err != nil {
		return nil, err
	}
	node, err = balanced.Layout(db)
	if err != nil {
		return nil, err
	}
	return
}
func (b *FSBuilder) Build() (*fsNode, error) {
	fsn, err := unixfs.FSNodeFromBytes(b.root.Data())
	if err != nil {
		return nil, xerrors.Errorf("input dag is not a unixfs node: %s", err)
	}

	rootn := &fsNode{
		Hash: b.root.Cid().String(),
		Size: fsn.FileSize(),
		Link: []fsNode{},
	}
	if !fsn.IsDir() {
		return rootn, nil
	}
	for _, ln := range b.root.Links() {
		fn, err := b.getNodeByLink(ln)
		if err != nil {
			return nil, err
		}
		rootn.Link = append(rootn.Link, fn)
	}

	return rootn, nil
}
type fsNode struct {
	Name string
	Hash string
	Size uint64
	Link []fsNode
}
func (b *FSBuilder) getNodeByLink(ln *format.Link) (fn fsNode, err error) {
	ctx := context.Background()
	fn = fsNode{
		Name: ln.Name,
		Hash: ln.Cid.String(),
		Size: ln.Size,
	}
	nd, err := b.ds.Get(ctx, ln.Cid)
	if err != nil {
		logger.Warn(err)
		return
	}

	nnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		err = xerrors.Errorf("failed to transformed to dag.ProtoNode")
		return
	}
	fsn, err := unixfs.FSNodeFromBytes(nnd.Data())
	if err != nil {
		logger.Warnf("input dag is not a unixfs node: %s", err)
		return
	}
	if !fsn.IsDir() {
		return
	}
	for _, ln := range nnd.Links() {
		node, err := b.getNodeByLink(ln)
		if err != nil {
			return node, err
		}
		fn.Link = append(fn.Link, node)
	}
	return
}