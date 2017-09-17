// Author: Jishan Baig

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include<string>
#include<iostream>

#define prepared 0
#define prepare 1
#define commit 2
 
using namespace std;

int main(int argc, char** argv) {
 //string result;
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);
  // Find out rank, size
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // We are assuming at least 2 processes for this task
  if (world_size < 3) {
    fprintf(stderr, "World size must be greater than 3 for %s\n", argv[0]);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  int number;
  //printf("%d\n",world_rank);
  if (world_rank == 0) 
  {
    printf("I am the Co-ordinator\n");
    int numofps=4;
    int stats[numofps];
    //receiving prepared from requsting node 2.
    //MPI_Recv(&number, 1, MPI_INT, 2, prepared, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    //receiving a string starts from here.
    MPI_Status status;
    MPI_Probe(2,prepared,MPI_COMM_WORLD, &status);
    int l;// = status.Get_count(MPI_CHAR);
    MPI_Get_count(&status, MPI_CHAR, &l);
    char *buf = new char[l];
    MPI_Recv(buf, l, MPI_CHAR, 2, prepared, MPI_COMM_WORLD, &status);
    string truestr(buf, l);
    printf("Process 0 received prepared from process 2 with transaction :: %s\n",truestr.c_str());
    //receiving a string ends here.
    delete [] buf;
    stats[2]=1;
    //sending to prepare to all other processes i.e. 1.
    for(int i=0;i<numofps;i++)
      if(i!=0 && i!=2)
         {
          MPI_Send(&number, 1, MPI_INT, i, prepare, MPI_COMM_WORLD);
          printf("Process 0 sent prepare to process %d\n",i);
         }   
    //receiving prepared from all other processes i.e. 1.
    for(int m=0;m<numofps;m++)
      {
       if(m!=0 && m!=2)
         {
         MPI_Recv(&number, 1, MPI_INT, m, prepared, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
         stats[m]=number;
         printf("Process 0 received prepared from process %d\n",m);
         }    
      }
    stats[0]=number;
    //making decision
    int dec=1; 
    for(int k=1;k<numofps;k++)
       {
         if(stats[k]==0)
           dec=0;
       }
    //and sending commit/abort according to the decision.
    if(dec==1)
     {
       for(int j=0;j<numofps;j++)
         if(j!=0) 
          {
           MPI_Send(truestr.c_str(), truestr.size(), MPI_CHAR, j, commit, MPI_COMM_WORLD);
           printf("Process 0 sent commit to process %d\n",j);
          }       
     }
     else
     {
       truestr="0";
       for(int j=0;j<numofps;j++)
         if(j!=0) 
          {
         MPI_Send(truestr.c_str(), truestr.size(), MPI_CHAR, j, commit, MPI_COMM_WORLD);
         printf("Process 0 sent abort to process %d\n",j);
          }
     }
     printf("Process 0 has completed its task\n");
  } 

  else if (world_rank == 2) 
  {
    printf("I am the Resource Manager-2\n");  
   //take transaction
   printf("Enter the transaction you want to perform : ");
   string trans;
   getline(cin,trans);
   //cout<<trans<<endl;
   //printf("hi");
   //sending prepared to co-ordinator.
   //MPI_Send(&number, 1, MPI_INT, 0, prepared, MPI_COMM_WORLD);
   MPI_Send(trans.c_str(), trans.size(), MPI_CHAR, 0, prepared, MPI_COMM_WORLD);
   printf("Process 2 sent prepared with transaction detail to process 0\n");
   //process of receiving starts.
    MPI_Status status;
    MPI_Probe(0, commit,MPI_COMM_WORLD, &status);
    int l;// = status.Get_count(MPI_CHAR);
    MPI_Get_count(&status, MPI_CHAR, &l);
    char *buf = new char[l];
    MPI_Recv(buf, l, MPI_CHAR, 0, commit, MPI_COMM_WORLD, &status);
    string getby_2(buf, l);
    delete [] buf;
    //process of receiving ends.
   //do commit/abort
   if(getby_2.size()>1)
      {
      printf("Process 2 received string %s from process 0\n",getby_2.c_str());
      printf("Process 2 has committed\n");
      }
    else
      printf("Process 2 has aborted the transaction\n");
  }
 else //world_rank !=2 and world_rank!=0 i.e. other resource managers.
  {
    printf("I am the Resource Manager-%d\n",world_rank);
    MPI_Recv(&number, 1, MPI_INT, 0, prepare, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("Process %d received prepare from process 0\n",world_rank);
    //Process 1 can send either number 0(not prep) or number 1(prep).
    number=1; //number=1;
    MPI_Send(&number, 1, MPI_INT, 0, prepared, MPI_COMM_WORLD);
    printf("Process %d sent prepared to process 0\n",world_rank);
    //process of receiving starts.
    MPI_Status status;
    MPI_Probe(0, commit,MPI_COMM_WORLD, &status);
    int l;// = status.Get_count(MPI_CHAR);
    MPI_Get_count(&status, MPI_CHAR, &l);
    char *buf = new char[l];
    MPI_Recv(buf, l, MPI_CHAR, 0, commit, MPI_COMM_WORLD, &status);
    string getby_1(buf, l);
    delete [] buf;
    //process of receiving ends.
    //do commit/abort
    if(getby_1.size()>1)
      {
      printf("Process %d received string %s from process 0\n",world_rank,getby_1.c_str());       
      printf("Process %d has committed\n",world_rank);
      }
    else
      printf("Process %d has aborted\n",world_rank);
  }
  
  MPI_Finalize();
}






