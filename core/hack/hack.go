// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package hack

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

func Red(format string,a... interface{}) {
	f,b,d := 31,40,1
	fmt.Printf(" %c[%d;%d;%dm(%s)%c[0m \n", 0x1B, f, b, d, fmt.Sprintf(format, a...)  ,0x1B)
}
func Yell(format string,a... interface{}) {
	f,b,d := 33,40,1
	fmt.Printf(" %c[%d;%d;%dm(%s)%c[0m \n", 0x1B, f, b, d, fmt.Sprintf(format, a...)  ,0x1B)
}

func Blue(format string,a... interface{}) {
	f,b,d := 34,47,1
	fmt.Printf(" %c[%d;%d;%dm(%s)%c[0m \n", 0x1B, f, b, d, fmt.Sprintf(format, a...)  ,0x1B)
}

func C(f,b,d int,format string,a... interface{}) {
	fmt.Printf(" %c[%d;%d;%dm(%s)%c[0m \n", 0x1B, f, b, d, fmt.Sprintf(format, a...)  ,0x1B)
}

// no copy to change slice to string
// use your own risk
func String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// no copy to change string to slice
// use your own risk
func Slice(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}

func IsSqlSep(r rune) bool {
	return r == ' ' || r == ',' ||
		r == '\t' || r == '/' ||
		r == '\n' || r == '\r'
}

func ArrayToString(array []int) string {
	if len(array) == 0 {
		return ""
	}
	var strArray []string
	for _, v := range array {
		strArray = append(strArray, strconv.FormatInt(int64(v), 10))
	}

	return strings.Join(strArray, ", ")
}
