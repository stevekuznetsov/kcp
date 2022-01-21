/*
Copyright 2022 The KCP Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workspacelock

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	jsonpatch "github.com/evanphx/json-patch"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clusters"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	tenancyv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/tenancy/v1alpha1"
	kcpclient "github.com/kcp-dev/kcp/pkg/client/clientset/versioned"
	tenancyinformer "github.com/kcp-dev/kcp/pkg/client/informers/externalversions/tenancy/v1alpha1"
	tenancylister "github.com/kcp-dev/kcp/pkg/client/listers/tenancy/v1alpha1"
)

const (
	lockedWorkspaceIndex = "locked"
	controllerName       = "workspacelock"
)

func NewController(
	identifier string,
	kcpClient kcpclient.ClusterInterface,
	workspaceWriteLockInformer tenancyinformer.WorkspaceWriteLockInformer,
) (*Controller, error) {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	c := &Controller{
		identifier:                identifier,
		queue:                     queue,
		kcpClient:                 kcpClient,
		workspaceWriteLockIndexer: workspaceWriteLockInformer.Informer().GetIndexer(),
		workspaceWriteLockLister:  workspaceWriteLockInformer.Lister(),
		syncChecks: []cache.InformerSynced{
			workspaceWriteLockInformer.Informer().HasSynced,
		},
	}

	workspaceWriteLockInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueue(obj) },
		UpdateFunc: func(_, obj interface{}) { c.enqueue(obj) },
	})

	if err := c.workspaceWriteLockIndexer.AddIndexers(map[string]cache.IndexFunc{
		lockedWorkspaceIndex: func(obj interface{}) ([]string, error) {
			if writeLock, ok := obj.(*tenancyv1alpha1.WorkspaceWriteLock); ok {
				return []string{strconv.FormatBool(writeLock.Spec.Locked && len(writeLock.Status.Acknowledgements) == writeLock.Spec.Quorum)}, nil
			}
			return []string{}, nil
		},
	}); err != nil {
		return nil, fmt.Errorf("failed to add indexer for WorkspaceWriteLock: %w", err)
	}

	return c, nil
}

// Controller watches Workspaces in order to make sure we disallow writes to ones that are locked.
type Controller struct {
	identifier string

	kcpClient kcpclient.ClusterInterface

	queue                     workqueue.RateLimitingInterface
	workspaceWriteLockIndexer cache.Indexer
	workspaceWriteLockLister  tenancylister.WorkspaceWriteLockLister

	syncChecks []cache.InformerSynced
}

func (c *Controller) enqueue(obj interface{}) {
	writeLock, ok := obj.(*tenancyv1alpha1.WorkspaceWriteLock)
	if !ok {
		runtime.HandleError(fmt.Errorf("got %T when handling WorkspaceWriteLock", obj))
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(writeLock)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	klog.Infof("queueing writeLock %q", key)
	c.queue.Add(key)
}

func (c *Controller) Start(ctx context.Context, numThreads int) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Infof("Starting %s controller", controllerName)
	defer klog.Infof("Shutting down %s controller", controllerName)

	if !cache.WaitForNamedCacheSync(controllerName, ctx.Done(), c.syncChecks...) {
		klog.Warning("Failed to wait for caches to sync")
		return
	}

	for i := 0; i < numThreads; i++ {
		go wait.Until(func() { c.startWorker(ctx) }, time.Second, ctx.Done())
	}

	<-ctx.Done()
}

func (c *Controller) startWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	// Wait until there is a new item in the working queue
	k, quit := c.queue.Get()
	if quit {
		return false
	}
	key := k.(string)

	klog.Infof("processing key %q", key)

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(key)

	if err := c.process(ctx, key); err != nil {
		runtime.HandleError(fmt.Errorf("%q controller failed to sync %q, err: %w", controllerName, key, err))
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *Controller) process(ctx context.Context, key string) error {
	namespace, clusterAwareName, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Errorf("invalid key: %q: %v", key, err)
		return nil
	}
	if namespace != "" {
		klog.Errorf("namespace %q found in key for cluster-wide Workspace object", namespace)
		return nil
	}
	clusterName, name := clusters.SplitClusterAwareKey(clusterAwareName)

	workspaceWriteLock, err := c.workspaceWriteLockLister.Get(key) // TODO: clients need a way to scope down the lister per-cluster
	if err != nil {
		if errors.IsNotFound(err) {
			return nil // object deleted before we handled it
		}
		return err
	}
	previous := workspaceWriteLock
	workspaceWriteLock = workspaceWriteLock.DeepCopy()

	if err := c.reconcile(ctx, workspaceWriteLock); err != nil {
		return err
	}

	// If the object being reconciled changed as a result, update it.
	if !equality.Semantic.DeepEqual(previous.Status, workspaceWriteLock.Status) {
		oldData, err := json.Marshal(tenancyv1alpha1.WorkspaceWriteLock{
			Status: previous.Status,
		})
		if err != nil {
			return fmt.Errorf("failed to Marshal old data for workspaceWriteLock %q|%q/%q: %w", clusterName, namespace, name, err)
		}

		newData, err := json.Marshal(tenancyv1alpha1.WorkspaceWriteLock{
			ObjectMeta: metav1.ObjectMeta{
				UID:             previous.UID,
				ResourceVersion: previous.ResourceVersion,
			}, // to ensure they appear in the patch as preconditions
			Status: workspaceWriteLock.Status,
		})
		if err != nil {
			return fmt.Errorf("failed to Marshal new data for workspaceWriteLock %q|%q/%q: %w", clusterName, namespace, name, err)
		}

		patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
		if err != nil {
			return fmt.Errorf("failed to create patch for workspaceWriteLock %q|%q/%q: %w", clusterName, namespace, name, err)
		}
		_, uerr := c.kcpClient.Cluster(clusterName).TenancyV1alpha1().Workspaces().Patch(ctx, workspaceWriteLock.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
		return uerr
	}

	return nil
}

func (c *Controller) reconcile(ctx context.Context, workspaceWriteLock *tenancyv1alpha1.WorkspaceWriteLock) error {
	var found bool
	for _, ack := range workspaceWriteLock.Status.Acknowledgements {
		if ack.Identifier == c.identifier {
			found = true
			break
		}
	}
	if !found {
		// TODO: make the write lock handler active
		list, err := c.kcpClient.Cluster(workspaceWriteLock.ClusterName).TenancyV1alpha1().WorkspaceWriteLocks().List(ctx, metav1.ListOptions{
			Limit: 1,
		})
		if err != nil {
			return fmt.Errorf("failed to determine current resourceVersion after locking: %w", err)
		}
		klog.Infof("locked API replica %s for workspaceWriteLock %s at %s", c.identifier, workspaceWriteLock.Name, list.ResourceVersion)
		workspaceWriteLock.Status.Acknowledgements = append(workspaceWriteLock.Status.Acknowledgements, tenancyv1alpha1.WorkspaceWriteLockAcknowledgement{
			Identifier:      c.identifier,
			ResourceVersion: list.ResourceVersion,
		})
	}

	if len(workspaceWriteLock.Status.Acknowledgements) == workspaceWriteLock.Spec.Quorum {
		var liveBeforeResourceVersion int64
		for i, ack := range workspaceWriteLock.Status.Acknowledgements {
			resourceVersion, err := strconv.ParseInt(ack.ResourceVersion, 10, 64)
			if err != nil {
				klog.Errorf("failed to parse workspaceWriteLock.status.acknowledgements[%d].resourceVersion: %v", i, err)
				return nil
			}
			if resourceVersion > liveBeforeResourceVersion {
				liveBeforeResourceVersion = resourceVersion
			}
		}
		klog.Infof("reached quorum for workspaceWriteLock %s at resourceVersion %s", workspaceWriteLock.Name, liveBeforeResourceVersion)
		// TODO: call out to org workspace to record liveBeforeResourceVersion
	}
	return nil
}
