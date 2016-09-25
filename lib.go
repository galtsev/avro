package avro

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func zencode(v int) uint64 {
	//return uint64((v >> 63) ^ (v << 1))
	if v >= 0 {
		return uint64(v) << 1
	} else {
		return (uint64(-v) << 1) - 1
	}
}

func zdecode(v uint64) int {
	//return int((v >> 1) ^ -(v & 1))
	if (v & 1) == 0 {
		return int(v >> 1)
	} else {
		return -int((v + 1) >> 1)
	}
}
