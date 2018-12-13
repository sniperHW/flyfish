package main 

import(
	"fmt"
)


func parseString(v string) (string,error) {
	s := len(v)
	if s > 1 {
		if v[0] == '"' && v[s-1] == '"' {
			return v[1:s-1],nil
		} else {
			return "",fmt.Errorf("invaild string")
		}
	}
	return "",fmt.Errorf("invaild string")
}

func main() {

	s := `""`

	fmt.Println(parseString(s))

	/*l := len(s)

	fmt.Println(s[l-2:],s[:l-2])

	fmt.Println(s[0])*/


}