package util

type ListNode struct{
	next *ListNode
	data interface{}
}


type List struct{
	head *ListNode
	tail *ListNode
	size  int64
}

func NewList()(*List){
	return &List{head:nil,tail:nil,size:0}
}

func (this *List) Push(data interface{}) {
	element := &ListNode{next:nil,data:data}
	if this.head == nil && this.tail == nil {
		this.head = element
		this.tail = element
	}else{
		this.tail.next = element
		this.tail = element
	}
	this.size += 1
}

func (this *List) Pop() interface{} {
	if this.head == nil {
		return nil
	}else{
		front := this.head
		this.head = this.head.next
		if this.head == nil {
			this.tail = nil
		}
		this.size -= 1
		return front.data
	}
}

func (this *List) Front() interface {} {
	if this.head == nil {
		return nil
	}else{
		return this.head.data
	}	
}

func (this *List) Clear() {
	this.head = nil
	this.tail = nil
	this.size = 0
}

func (this *List) Len() int64 {
	return this.size
}

func (this *List) Empty() bool {
	return this.size == 0
}









