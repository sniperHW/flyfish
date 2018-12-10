package flyfish

type keyCmdQueue struct {
	head       *command
	tail       *command
	size       int 	
	locked 	   bool     	
}

func (this *keyCmdQueue) Push(cmd *command) {
	if nil != this.tail {
		this.tail.next = cmd
		this.tail = cmd
	} else {
		this.head = cmd
		this.tail = cmd
	}
	this.size++
}

func (this *keyCmdQueue) Pop() *command {
	if nil == this.head {
		return nil
	} else {
		head := this.head
		this.head = head.next
		if this.head == nil {
			this.tail = nil
		}
		this.size--
		return head
	}
}

func (this *keyCmdQueue) Head() *command {
	return this.head
}