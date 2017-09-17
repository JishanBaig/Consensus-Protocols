// Author: Jishan Baig

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include<string>
#include<iostream>
#include<fstream>
#include<sstream>
#include<vector>
#include<iterator>
#include<algorithm>

#define prepared 0
#define prepare 1
#define commit 2
#define byz_start 3
#define byz 4
#define byz_end 5

 
using namespace std;


void tocomm(string request)
{
int reqline,b,i;
ifstream infile("store.txt");
ofstream outfile("res.txt");
stringstream is;
string line,trans;
string word1,word2,act_str;
vector<string> v,tokens;
vector<string>::iterator it;
istringstream iss(request);
copy(istream_iterator<string>(iss),istream_iterator<string>(),back_inserter(tokens));
for(it=tokens.begin();it!=tokens.end();it++)  
cout<<*it<<" "<<endl;

for(int lineNum = 0; getline(infile, line); lineNum++)
{
    stringstream ss(line);
    string word;
    ss>>word; 
    if(word.compare(tokens[0])==0)
      {
         reqline=lineNum;
         word1=word;;
         ss >> word;
         word2=word;
         if(tokens[1].compare("add")==0) 
            b = atoi(word2.c_str())+atoi(tokens[2].c_str());
         else
            b = atoi(word2.c_str())-atoi(tokens[2].c_str());
         is<<b;
         act_str=word1+' '+is.str();
         //cout<<act_str<<endl;
         v.push_back(act_str);
       }
     else
       v.push_back(line);
}
printf("line to be changed : %d\n",reqline);
for(i=0;i<v.size();i++)
outfile<<v[i]<<endl;
outfile.close();
}


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
  if (world_size < 5) {
    fprintf(stderr, "World size must be greater than 2 for %s\n", argv[0]);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  int numofps=4;
  int number;
  //printf("%d\n",world_rank);
  if (world_rank == 0) 
  {
    printf("I am the Co-ordinator\n");
   
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
    //making decision and apply Byzantine.
    int dec=1; 
    for(int k=1;k<numofps;k++)
       {
         if(stats[k]==0)
           dec=0;
       }
    for(int x=0;x<numofps;x++)
      if(x!=0)
         {
          MPI_Send(&dec, 1, MPI_INT, x, byz_start, MPI_COMM_WORLD);
          printf("Process 0 sent byz_start to process %d\n",x);
         }   
     int final_stats[numofps];
     for(int y=0;y<numofps;y++)
      if(y!=0)
         {
         MPI_Recv(&number, 1, MPI_INT, y, byz_end, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
         final_stats[y]=number;
         printf("Process 0 received byz_end from process %d\n",y);
         }    
      int final_dec=1; 
      for(int z=1;z<numofps;z++)
       {
         if(stats[z]==0)
           final_dec=0;
       }
    //and sending commit/abort according to the decision.
    if(final_dec==1)
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
   //byz_start.
    int dec,final_dec;
    MPI_Recv(&dec, 1, MPI_INT, 0, byz_start, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("Process %d received byz_start from process 0\n",world_rank);
    //Byzantine Protocol starts.
    int steps=2,nZ=0,nO=0;
    int pstats[numofps];
    pstats[world_rank]=dec;
    while(steps--)
    {
     for(int x=1;x<numofps;x++)
      if(x!=world_rank)
         {
          MPI_Send(&dec, 1, MPI_INT, x, byz, MPI_COMM_WORLD);
          printf("Process 0 sent byz_%d to process %d\n",steps,x);
         }  
     for(int y=1;y<numofps;y++)
      if(y!=world_rank)
         {
         MPI_Recv(&number, 1, MPI_INT, y, byz, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
         pstats[y]=number;
         printf("Process 0 received byz_%d from process %d\n",steps,y);
         }     
     for(int k=1;k<numofps;k++)
       {
         printf("%d ",pstats[k]);
         if(pstats[k]==0)
           nZ++;
         else
           nO++;
       }
    printf("\n");
    if(nZ>nO)
       dec=0;
    else
       dec=1;
   }
   //while loop ends here.
   final_dec=dec;
    //byz_end.
    MPI_Send(&final_dec, 1, MPI_INT, 0, byz_end, MPI_COMM_WORLD);
    printf("Process %d sent byz_end to process 0\n",world_rank);
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
      tocomm(getby_2);
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
    //byz_start.
    int dec,final_dec;
    MPI_Recv(&dec, 1, MPI_INT, 0, byz_start, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("Process %d received byz_start from process 0\n",world_rank);
    //Byzantine Protocol starts.
    int steps=2,nZ=0,nO=0;
    int pstats[numofps];
    //faulty one.
    if(world_rank==3) dec=0;
    pstats[world_rank]=dec;
    while(steps--)
    {
     for(int x=1;x<numofps;x++)
      if(x!=world_rank)
         {if(world_rank==3) dec=0;
          MPI_Send(&dec, 1, MPI_INT, x, byz, MPI_COMM_WORLD);
          printf("Process 0 sent byz_%d to process %d\n",steps,x);
         }  
     for(int y=1;y<numofps;y++)
      if(y!=world_rank)
         {
         MPI_Recv(&number, 1, MPI_INT, y, byz, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
         pstats[y]=number;
         printf("Process 0 received byz_%d from process %d\n",steps,y);
         }     
     for(int k=1;k<numofps;k++)
       {
         printf("%d ",pstats[k]);
         if(pstats[k]==0)
           nZ++;
         else
           nO++;
       }
    printf("\n");
    if(nZ>nO)
       dec=0;
    else
       dec=1;
    }
    //while loop ends here.
    final_dec=dec;
    //byz_end.
    MPI_Send(&final_dec, 1, MPI_INT, 0, byz_end, MPI_COMM_WORLD);
    printf("Process %d sent byz_end to process 0\n",world_rank);
    //process of receiving starts the transaction.
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






