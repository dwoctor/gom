package gom

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// A mongodb database collection. 
type Collection struct {
	address, database, collection string
}

// Creates a new mongodb database collection.
func NewCollection(address, database, collection string) *Collection {
	return &Collection{address: address, database: database, collection: collection}
}

// Puts a record into the collection.
func (this *Collection) Put(record interface{}) error {
	session, err := mgo.Dial(this.address)
	if err != nil {
		return err
	}
	defer session.Close()
	collection := session.DB(this.database).C(this.collection)
	if err = collection.Insert(record); err != nil {
		return err
	}
	return nil
}

// Gets a record into the collection
func (this *Collection) Get(fields interface{}) (*bson.Raw, error) {
	session, err := mgo.Dial(this.address)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	session.SetSafe(&mgo.Safe{})
	collection := session.DB(this.database).C(this.collection)
	var result bson.Raw
	if err = collection.Find(fields).Limit(1).One(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Does a record in the collection containing these fields exist.
func (this *Collection) Has(fields interface{}) (bool, error) {
	session, err := mgo.Dial(this.address)
	if err != nil {
		return false, err
	}
	defer session.Close()
	session.SetSafe(&mgo.Safe{})
	collection := session.DB(this.database).C(this.collection)
	if count, err := collection.Find(fields).Limit(1).Count(); err != nil {
		return false, err
	} else if count != 1 {
		return false, nil
	} else {
		return true, nil
	}
}

// Perfoms a Has and Get operation.
func (this *Collection) Fetch(fields interface{}) (*bson.Raw, error) {
	if found, err := this.Has(fields); err != nil {
		return nil, err
	} else if found == false {
		return nil, nil
	} else if record, err := this.Get(fields); err != nil {
		return nil, err
	} else {
		return record, nil
	}
}

// Drops the collection deleting all the data.
func (this *Collection) Drop() error {
	session, err := mgo.Dial(this.address)
	if err != nil {
		return err
	}
	defer session.Close()
	if err = session.DB(this.database).C(this.collection).DropCollection(); err != nil {
		return err
	}
	return nil
}
