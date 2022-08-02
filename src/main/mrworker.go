package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"6.824/mr"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode"
)

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
//func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
//	p, err := plugin.Open(filename)
//	if err != nil {
//		log.Fatalf("cannot load plugin %v", filename)
//	}
//	xmapf, err := p.Lookup("Map")
//	if err != nil {
//		log.Fatalf("cannot find Map in %v", filename)
//	}
//	mapf := xmapf.(func(string, string) []mr.KeyValue)
//	xreducef, err := p.Lookup("Reduce")
//	if err != nil {
//		log.Fatalf("cannot find Reduce in %v", filename)
//	}
//	reducef := xreducef.(func(string, []string) string)
//
//	return mapf, reducef
//}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	//mapf := func(filename string, contents string) []mr.KeyValue {
	//	// function to detect word separators.
	//	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	//
	//	// split contents into an array of words.
	//	words := strings.FieldsFunc(contents, ff)
	//
	//	kva := []mr.KeyValue{}
	//	for _, w := range words {
	//		kv := mr.KeyValue{w, "1"}
	//		kva = append(kva, kv)
	//	}
	//	return kva
	//}
	//
	////
	//// The reduce function is called once for each key generated by the
	//// map tasks, with a list of all the values created for that key by
	//// any map task.
	////
	//reducef := func(key string, values []string) string {
	//	// return the number of occurrences of this word.
	//	return strconv.Itoa(len(values))
	//}
	//

	mapf := func(document string, value string) (res []mr.KeyValue) {
		m := make(map[string]bool)
		words := strings.FieldsFunc(value, func(x rune) bool { return !unicode.IsLetter(x) })
		for _, w := range words {
			m[w] = true
		}
		for w := range m {
			kv := mr.KeyValue{w, document}
			res = append(res, kv)
		}
		return
	}

	// The reduce function is called once for each key generated by Map, with a
	// list of that key's string value (merged across all inputs). The return value
	// should be a single output value for that key.
	reducef := func(key string, values []string) string {
		sort.Strings(values)
		return fmt.Sprintf("%d %s", len(values), strings.Join(values, ","))
	}
	return mapf, reducef

}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	//
	// The reduce function is called once for each key generated by the
	// map tasks, with a list of all the values created for that key by
	// any map task.
	//

	mr.Worker(mapf, reducef)
}
