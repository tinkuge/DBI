#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include<iostream>
#include<string.h>

// stub file .. replace it with your own DBFile.cc
using namespace std;
DBFile::DBFile () {
    //initialize the file and page object pointers
    f = new File();
    p = new Page();
    //set dirty page to false
    pdirty = false;
    //set pagenumber to 1, page 0 yields errors?
    pnum =1;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    //get the file path from constant char pointer to char pointer
    char *cf  = strdup(f_path);
    //Open the file using the file pointer
    f->Open(0,cf);
    //cout<<create
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    //Open the file from the given path using fopen (String.h)
    fload = fopen(loadpath, "r");
    //cout<<"Load"
    //if fopen yields null pointer, file couldn't be opened
    if(fload == nullptr){
        cout<<"File could not be opened"<<endl;
        return;
    }

    else{
        //else, continue to suck the next record from the given schema
        while(true){
            int ret = r.SuckNextRecord(&f_schema,fload);
            if(ret == 1){
                //if the sucknextrecord is successful, add the subsequent record
                Add(r);
            }
            else
            {
                //if not, terminate the loop
                break;
            }
        }
    }
}

int DBFile::Open (const char *f_path) {
    //get the file path from constant char pointer to char pointer
    char *fp = strdup(f_path);
    //open the corresponding file
    f->Open(1,fp);
    //set page number to 1 since it should read from beginning
    pnum = 1;
    //cout << "open" << endl;
    return 1;
}

void DBFile::MoveFirst () {
    //cout << "move" << endl;
    //Get the first page
    f->GetPage(p,0);
    //cout << "1" << endl;
    pnum = 1;

}

int DBFile::Close () {
    //if the page is dirty at the time of closing, write it out
    if(pdirty == true){
        f->AddPage(p,pnum);
        p->EmptyItOut();
    }
    f->Close();

}

void DBFile::Add (Record &rec) {
    //append the current record to the current page
    int ret = p->Append(&rec);
    if(ret == 1){
        //set pdirty as true if it hasn't been true
        pdirty = true;
        //cout<<"Record appended";
    }
    else{
        //Page is full, write the current page out and write the current record to next page
        //cout<<"Page full";
        f->AddPage(p,pnum);
        p->EmptyItOut();
        //since page is empty, set pdirty to false
        pdirty = false;
        //new page number is one more than prev
        pnum = pnum + 1;
        //Append the record to the new page
        p->Append(&rec);
        //Set pdirty to true since a record has been added
        pdirty = true;
    }
}

int DBFile::GetNext (Record &fetchme) {
    //Get the first record of the page
    //Doesn't really return the next record of `fetchme` as there is no functionality in
    //Page class to go the particular referenced record without ejecting all the preceding records
    int ret = p->GetFirst(&fetchme);
    // cout << file->GetLength() << endl;
    // cout << ret << endl;
    
    //if a record exists in the page
    if(ret != 0){
        return ret;
    }
    else{
        pnum = pnum + 1;
        //empty the page (kinda redundant)
        p->EmptyItOut();
        
        //if the page number doesn't exceed the overall number of pages in file
        if(pnum < f->GetLength()-1){
            //get the page
            f->GetPage(p,pnum);
            
            //store return value of new page in newret
            int newret = p->GetFirst(&fetchme);
            if(newret!=0){
                //if the newly fetched page has a record
                return newret;
            }
            //New page's first record is empty
            else{
                return 0;
            }
        }
        //if no more pages exist, after the last page is emptied
        else{
            return 0;
        }
    }

}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine ce;
    //keep iterating through the records in the page until a record satisfies the cnf
    while(GetNext(fetchme)){
        //compare the current record with the given schema
        if(ce.Compare(&fetchme,&literal,&cnf)){
            return 1;
        }
    }
    //no match
    return 0;
}
