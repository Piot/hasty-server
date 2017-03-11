package debug

import "fmt"

func Map(reason string, m map[string]interface{}) {
	fmt.Printf("-------- MAP: %s ---------\n", reason)
	//for key, value := range m {
	//	fmt.Printf("%s = %s\n", key, value)
	//}
	fmt.Printf("%v", m)
	fmt.Printf("----------------------------\n")
}

func Octets(reason string, octets []byte) {
	fmt.Printf("%s: %q", reason, octets)
}

func KeyValue(reason string, key string, value interface{}) {
	fmt.Printf("%s: %s %v\n", reason, key, value)
}
