package DataNode

import (
	"context"
	"slices"
	"testing"
)

func TestSingleDataNode_storeMultipleRecords(t *testing.T) {

	ctx := context.Background()
	size := 3

	n := (&SingleDataNode{}).New(ctx, size)

	keys := []string{
		"key1",
		"key2",
		"key3",
	}
	values := []any{
		"value1",
		"value2",
		"value3",
	}

	if err := n.storeMultipleRecords(keys, values); err != nil {
		t.Errorf("storeMultipleRecords() error = %v", err)
	}
	if len(keys) != n.Len() {
		t.Errorf("storeMultipleRecords() error, length must be %d, got %d", len(keys), n.Len())
	}

	n.storeMultipleRecords([]string{"key4"}, []any{"value4"})

	if size != n.Len() {
		t.Errorf("storeMultipleRecords() error, length must be %d, got %d", size, n.Len())
	}
	// expect "key1" to be evicted, keys 2,3,4 still there
	keys4 := append(keys, "key4")
	kf, _ := n.findMultipleKeys(keys4)

	if size != n.Len() {
		t.Errorf("storeMultipleRecords() error, length must be %d, got %d", size, n.Len())
	}

	if slices.Index(kf, "key1") > 0 {
		t.Errorf("storeMultipleRecords() error, key1 expected to be evicted")
	}
	if slices.Index(kf, "key2") < 0 {
		t.Errorf("storeMultipleRecords() error, key2 expected to be present")
	}
	if slices.Index(kf, "key3") < 0 {
		t.Errorf("storeMultipleRecords() error, key3 expected to be present")
	}
	if slices.Index(kf, "key4") < 0 {
		t.Errorf("storeMultipleRecords() error, key4 expected to be present")
	}

}

func TestSingleDataNode_findMultipleKeys(t *testing.T) {

	ctx := context.Background()
	size := 3

	n := (&SingleDataNode{}).New(ctx, size)

	keys := []string{
		"key1",
		"key2",
		"key3",
	}
	values := []any{
		"value1",
		"value2",
		"value3",
	}

	if err := n.storeMultipleRecords(keys, values); err != nil {
		t.Errorf("storeMultipleRecords() error = %v", err)
	}
	if len(keys) != n.Len() {
		t.Errorf("storeMultipleRecords() error, length must be %d, got %d", len(keys), n.Len())
	}

	n.storeMultipleRecords([]string{"key4"}, []any{"value4"})
	keys4 := append(keys, "key4")
	kf, vf := n.findMultipleKeys(keys4)

	if size != n.Len() {
		t.Errorf("storeMultipleRecords() error, length must be %d, got %d", size, n.Len())
	}

	ndx := -1

	if ndx = slices.Index(kf, "key2"); ndx < 0 {
		t.Errorf("findMultipleKeys() error, key2 expected to be present")
	}
	if vf[ndx] != "value2" {
		t.Errorf("findMultipleKeys() error, value2 expected, got %v", vf[ndx])
	}
	if ndx = slices.Index(kf, "key3"); ndx < 0 {
		t.Errorf("findMultipleKeys() error, key3 expected to be present")
	}
	if vf[ndx] != "value3" {
		t.Errorf("findMultipleKeys() error, value3 expected, got %v", vf[ndx])
	}
	if ndx = slices.Index(kf, "key4"); ndx < 0 {
		t.Errorf("findMultipleKeys() error, key4 expected to be present")
	}
	if vf[ndx] != "value4" {
		t.Errorf("findMultipleKeys() error, value4 expected, got %v", vf[ndx])
	}

}

func TestSingleDataNode_storeMultipleRecordsEviction(t *testing.T) {

	ctx := context.Background()
	size := 3

	n := (&SingleDataNode{}).New(ctx, size)

	keys := []string{
		"key1",
		"key2",
		"key3",
	}
	values := []any{
		"value1",
		"value2",
		"value3",
	}

	if err := n.storeMultipleRecords(keys, values); err != nil {
		t.Errorf("storeMultipleRecords() error = %v", err)
	}
	if len(keys) != n.Len() {
		t.Errorf("storeMultipleRecords() error, length must be %d, got %d", len(keys), n.Len())
	}

	_, _ = n.findMultipleKeys([]string{"key1"}) // touch key1, it should stay
	n.storeMultipleRecords([]string{"key4"}, []any{"value4"})

	if size != n.Len() {
		t.Errorf("storeMultipleRecords() error, length must be %d, got %d", size, n.Len())
	}

	// expect "key2" to be evicted, keys 1,3,4 still there
	keys4 := append(keys, "key4")
	kf, _ := n.findMultipleKeys(keys4)

	if slices.Index(kf, "key2") > 0 {
		t.Errorf("storeMultipleRecords() error, key2 expected to be evicted, got %v", kf)
	}
	if slices.Index(kf, "key1") < 0 {
		t.Errorf("storeMultipleRecords() error, key1 expected to be present, got %v", kf)
	}
	if slices.Index(kf, "key3") < 0 {
		t.Errorf("storeMultipleRecords() error, key3 expected to be present, got %v", kf)
	}
	if slices.Index(kf, "key4") < 0 {
		t.Errorf("storeMultipleRecords() error, key4 expected to be present, got %v", kf)
	}

}
