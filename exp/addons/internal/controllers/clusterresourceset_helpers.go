/*
Copyright 2020 The Kubernetes Authors.

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

package controllers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"unicode"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	addonsv1 "sigs.k8s.io/cluster-api/exp/addons/api/v1beta1"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"
	utilresource "sigs.k8s.io/cluster-api/util/resource"
	utilyaml "sigs.k8s.io/cluster-api/util/yaml"
)

var jsonListPrefix = []byte("[")

// isJSONList returns whether the data is in JSON list format.
func isJSONList(data []byte) (bool, error) {
	const peekSize = 32
	buffer := bufio.NewReaderSize(bytes.NewReader(data), peekSize)
	b, err := buffer.Peek(peekSize)
	if err != nil {
		return false, err
	}
	trim := bytes.TrimLeftFunc(b, unicode.IsSpace)
	return bytes.HasPrefix(trim, jsonListPrefix), nil
}

func apply(ctx context.Context, c client.Client, strategy addonsv1.ClusterResourceSetStrategy, data []byte) error {
	isJSONList, err := isJSONList(data)
	if err != nil {
		return err
	}
	objs := []unstructured.Unstructured{}
	// If it is a json list, convert each list element to an unstructured object.
	if isJSONList {
		var results []map[string]interface{}
		// Unmarshal the JSON to the interface.
		if err = json.Unmarshal(data, &results); err == nil {
			for i := range results {
				var u unstructured.Unstructured
				u.SetUnstructuredContent(results[i])
				objs = append(objs, u)
			}
		}
	} else {
		// If it is not a json list, data is either json or yaml format.
		objs, err = utilyaml.ToUnstructured(data)
		if err != nil {
			return errors.Wrapf(err, "failed converting data to unstructured objects")
		}
	}

	errList := []error{}
	sortedObjs := utilresource.SortForCreate(objs)
	for i := range sortedObjs {
		if err := applyUnstructured(ctx, c, strategy, &sortedObjs[i]); err != nil {
			errList = append(errList, err)
		}
	}
	return kerrors.NewAggregate(errList)
}

func applyUnstructured(ctx context.Context, c client.Client, strategy addonsv1.ClusterResourceSetStrategy, obj *unstructured.Unstructured) error {
	// Create the object on the API server.
	// TODO: Errors are only logged. If needed, exponential backoff or requeuing could be used here for remedying connection glitches etc.
	if err := c.Create(ctx, obj); err != nil {
		// The create call is idempotent, so if the object already exists
		// then do not consider it to be an error.
		if apierrors.IsAlreadyExists(err) {
			if strategy == addonsv1.ClusterResourceSetStrategyApplyAlways {
				// The object here has already been created, so attempt to
				// update the object here. first we must get the obj we are trying to update and get the ResourceVersion

				objKey := client.ObjectKey{
					Namespace: obj.GetNamespace(),
					Name:      obj.GetName(),
				}

				// this will be the object that currently exists in the api
				existingObj := &unstructured.Unstructured{}
				existingObj.SetAPIVersion(obj.GetAPIVersion())
				existingObj.SetKind(obj.GetKind())

				errGet := c.Get(ctx, objKey, existingObj)
				if errGet != nil {
					return wrapClientError(obj, "get", errGet)
				}
				obj.SetUID(existingObj.GetUID())
				obj.SetResourceVersion(existingObj.GetResourceVersion())

				errUpdating := c.Update(ctx, obj)
				if errUpdating != nil {
					return wrapClientError(obj, "update", errUpdating)
				}
			}
		} else {
			return wrapClientError(obj, "create", err)
		}
	}
	return nil
}

func wrapClientError(obj *unstructured.Unstructured, clientOperation string, err error) error {
	return errors.Wrapf(
		err,
		"failed to %s object %s %s/%s",
		clientOperation,
		obj.GroupVersionKind(),
		obj.GetNamespace(),
		obj.GetName())
}

// getOrCreateClusterResourceSetBinding retrieves ClusterResourceSetBinding resource owned by the cluster or create a new one if not found.
func (r *ClusterResourceSetReconciler) getOrCreateClusterResourceSetBinding(ctx context.Context, cluster *clusterv1.Cluster, clusterResourceSet *addonsv1.ClusterResourceSet) (*addonsv1.ClusterResourceSetBinding, error) {
	clusterResourceSetBinding := &addonsv1.ClusterResourceSetBinding{}
	clusterResourceSetBindingKey := client.ObjectKey{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}

	if err := r.Client.Get(ctx, clusterResourceSetBindingKey, clusterResourceSetBinding); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		clusterResourceSetBinding.Name = cluster.Name
		clusterResourceSetBinding.Namespace = cluster.Namespace
		clusterResourceSetBinding.OwnerReferences = ensureOwnerRefs(clusterResourceSetBinding, clusterResourceSet, cluster)
		clusterResourceSetBinding.Spec.Bindings = []*addonsv1.ResourceSetBinding{}

		if err := r.Client.Create(ctx, clusterResourceSetBinding); err != nil {
			if apierrors.IsAlreadyExists(err) {
				if err = r.Client.Get(ctx, clusterResourceSetBindingKey, clusterResourceSetBinding); err != nil {
					return nil, err
				}
				return clusterResourceSetBinding, nil
			}
			return nil, errors.Wrapf(err, "failed to create clusterResourceSetBinding for cluster: %s/%s", cluster.Namespace, cluster.Name)
		}
	}
	return clusterResourceSetBinding, nil
}

// ensureOwnerRefs ensures Cluster and ClusterResourceSet owner references are set on the ClusterResourceSetBinding.
func ensureOwnerRefs(clusterResourceSetBinding *addonsv1.ClusterResourceSetBinding, clusterResourceSet *addonsv1.ClusterResourceSet, cluster *clusterv1.Cluster) []metav1.OwnerReference {
	ownerRefs := make([]metav1.OwnerReference, len(clusterResourceSetBinding.GetOwnerReferences()))
	copy(ownerRefs, clusterResourceSetBinding.GetOwnerReferences())
	ownerRefs = util.EnsureOwnerRef(ownerRefs, metav1.OwnerReference{
		APIVersion: clusterv1.GroupVersion.String(),
		Kind:       "Cluster",
		Name:       cluster.Name,
		UID:        cluster.UID,
	})
	ownerRefs = util.EnsureOwnerRef(ownerRefs,
		metav1.OwnerReference{
			APIVersion: clusterResourceSet.GroupVersionKind().GroupVersion().String(),
			Kind:       clusterResourceSet.GroupVersionKind().Kind,
			Name:       clusterResourceSet.Name,
			UID:        clusterResourceSet.UID,
		})
	return ownerRefs
}

// getConfigMap retrieves any ConfigMap from the given name and namespace.
func getConfigMap(ctx context.Context, c client.Client, configmapName types.NamespacedName) (*corev1.ConfigMap, error) {
	configMap := &corev1.ConfigMap{}
	configMapKey := client.ObjectKey{
		Namespace: configmapName.Namespace,
		Name:      configmapName.Name,
	}
	if err := c.Get(ctx, configMapKey, configMap); err != nil {
		return nil, err
	}

	return configMap, nil
}

// getSecret retrieves any Secret from the given secret name and namespace.
func getSecret(ctx context.Context, c client.Client, secretName types.NamespacedName) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	secretKey := client.ObjectKey{
		Namespace: secretName.Namespace,
		Name:      secretName.Name,
	}
	if err := c.Get(ctx, secretKey, secret); err != nil {
		return nil, err
	}

	return secret, nil
}

func computeHash(dataArr [][]byte) string {
	hash := sha256.New()
	for i := range dataArr {
		_, err := hash.Write(dataArr[i])
		if err != nil {
			continue
		}
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil))
}

func getDataListAndHash(resourceKind string, unstructuredData map[string]interface{}, errList []error) ([][]byte, string) {
	// Since maps are not ordered, we need to order them to get the same hash at each reconcile.
	keys := make([]string, 0)

	for key := range unstructuredData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	dataList := make([][]byte, 0)
	for _, key := range keys {
		val, ok, err := unstructured.NestedString(unstructuredData, key)
		if !ok || err != nil {
			errList = append(errList, errors.New("failed to get value field from the resource"))
			continue
		}

		byteArr := []byte(val)
		// If the resource is a Secret, data needs to be decoded.
		if resourceKind == string(addonsv1.SecretClusterResourceSetResourceKind) {
			byteArr, _ = base64.StdEncoding.DecodeString(val)
		}

		dataList = append(dataList, byteArr)
	}

	return dataList, computeHash(dataList)
}

func handleGetResourceErrors(clusterResourceSet *addonsv1.ClusterResourceSet, err error, errList []error) {
	if err == ErrSecretTypeNotSupported {
		conditions.MarkFalse(clusterResourceSet, addonsv1.ResourcesAppliedCondition, addonsv1.WrongSecretTypeReason, clusterv1.ConditionSeverityWarning, err.Error())
	} else {
		conditions.MarkFalse(clusterResourceSet, addonsv1.ResourcesAppliedCondition, addonsv1.RetrievingResourceFailedReason, clusterv1.ConditionSeverityWarning, err.Error())

		// Continue without adding the error to the aggregate if we can't find the resource.
		if apierrors.IsNotFound(err) {
			return
		}
	}
	errList = append(errList, err)
}
