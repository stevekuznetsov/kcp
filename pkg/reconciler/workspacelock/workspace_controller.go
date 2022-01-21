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
	controllerNameProducer = "workspacelockproducer"
)

func NewProducingController(
	identifier string,
	quorum int,
	kcpClient kcpclient.ClusterInterface,
	workspaceInformer tenancyinformer.WorkspaceInformer,
	workspaceWriteLockInformer tenancyinformer.WorkspaceWriteLockInformer,
) *ProducingController {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	c := &ProducingController{
		identifier:               identifier,
		quorum:                   quorum,
		queue:                    queue,
		kcpClient:                kcpClient,
		workspaceLister:          workspaceInformer.Lister(),
		workspaceWriteLockLister: workspaceWriteLockInformer.Lister(),
		syncChecks: []cache.InformerSynced{
			workspaceInformer.Informer().HasSynced,
			workspaceWriteLockInformer.Informer().HasSynced,
		},
	}

	workspaceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueue(obj) },
		UpdateFunc: func(_, obj interface{}) { c.enqueue(obj) },
	})

	return c
}

// ProducingController watches Workspaces in order to make sure we record whether the
// shard we are running on should be locked or not. This controller assumes that all
// the Workspaces we see are mirrored copies, and are present in our view if and only if
// the shard we are running on is where the Workspace is currently placed, or where it is
// migrating. We produce WorkspaceWriteLocks in the same (shard-local) logical cluster as
// the mirrored Workspaces, as we need to know our native resourceVersion around which we
// are locking.
type ProducingController struct {
	identifier string
	quorum     int

	kcpClient kcpclient.ClusterInterface

	queue                    workqueue.RateLimitingInterface
	workspaceLister          tenancylister.WorkspaceLister
	workspaceWriteLockLister tenancylister.WorkspaceWriteLockLister

	syncChecks []cache.InformerSynced
}

func (c *ProducingController) enqueue(obj interface{}) {
	workspace, ok := obj.(*tenancyv1alpha1.Workspace)
	if !ok {
		runtime.HandleError(fmt.Errorf("got %T when handling Workspace", obj))
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(workspace)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	klog.Infof("queueing workspace %q", key)
	c.queue.Add(key)
}

func (c *ProducingController) Start(ctx context.Context, numThreads int) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Infof("Starting %s controller", controllerNameProducer)
	defer klog.Infof("Shutting down %s controller", controllerNameProducer)

	if !cache.WaitForNamedCacheSync(controllerNameProducer, ctx.Done(), c.syncChecks...) {
		klog.Warning("Failed to wait for caches to sync")
		return
	}

	for i := 0; i < numThreads; i++ {
		go wait.Until(func() { c.startWorker(ctx) }, time.Second, ctx.Done())
	}

	<-ctx.Done()
}

func (c *ProducingController) startWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *ProducingController) processNextWorkItem(ctx context.Context) bool {
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
		runtime.HandleError(fmt.Errorf("%q controller failed to sync %q, err: %w", controllerNameProducer, key, err))
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *ProducingController) process(ctx context.Context, key string) error {
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

	workspace, err := c.workspaceLister.Get(key) // TODO: clients need a way to scope down the lister per-cluster
	if err != nil {
		if errors.IsNotFound(err) {
			return nil // object deleted before we handled it
		}
		return err
	}
	workspace = workspace.DeepCopy()

	expectedWriteLock := &tenancyv1alpha1.WorkspaceWriteLock{
		ObjectMeta: metav1.ObjectMeta{
			Name: workspace.Name,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: workspace.APIVersion,
				Kind:       workspace.Kind,
				Name:       workspace.Name,
				UID:        workspace.UID,
			}},
		},
		Spec: tenancyv1alpha1.WorkspaceWriteLockSpec{
			Locked: (workspace.Status.Location.Current == c.identifier) == workspace.Spec.ReadOnly, // names match and readOnly, or names don't match and not
			Quorum: c.quorum,
		},
	}

	previous, err := c.workspaceWriteLockLister.Get(key) // TODO: clients need a way to scope down the lister per-cluster
	if errors.IsNotFound(err) {
		// if we just need to create one, do it
		_, createErr := c.kcpClient.Cluster(clusterName).TenancyV1alpha1().WorkspaceWriteLocks().Create(ctx, expectedWriteLock, metav1.CreateOptions{})
		return createErr
	} else if err != nil {
		return err
	}

	// otherwise, see if we need to update the current one.
	if !equality.Semantic.DeepEqual(previous.Spec, expectedWriteLock.Spec) {
		oldData, err := json.Marshal(tenancyv1alpha1.WorkspaceWriteLock{
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: previous.OwnerReferences,
			},
			Spec: previous.Spec,
		})
		if err != nil {
			return fmt.Errorf("failed to Marshal old data for workspaceWriteLock %q|%q/%q: %w", clusterName, namespace, name, err)
		}

		newData, err := json.Marshal(tenancyv1alpha1.WorkspaceWriteLock{
			ObjectMeta: metav1.ObjectMeta{
				UID:             previous.UID,
				ResourceVersion: previous.ResourceVersion,
				OwnerReferences: expectedWriteLock.OwnerReferences,
			}, // to ensure they appear in the patch as preconditions
			Spec: expectedWriteLock.Spec,
		})
		if err != nil {
			return fmt.Errorf("failed to Marshal new data for workspaceWriteLock %q|%q/%q: %w", clusterName, namespace, name, err)
		}

		patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
		if err != nil {
			return fmt.Errorf("failed to create patch for workspaceWriteLock %q|%q/%q: %w", clusterName, namespace, name, err)
		}
		_, uerr := c.kcpClient.Cluster(clusterName).TenancyV1alpha1().Workspaces().Patch(ctx, expectedWriteLock.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
		return uerr
	}

	return nil
}
