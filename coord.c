#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/times.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

//struct to store records
struct records{
    int resID;
    char firstName[32];
    char lastName[32];
    int numDependents;
    float income;
    int postalCode;
};

int main(int argc, char* argv[]) {

    //read arguments
    char* filename=argv[0];
	int k=atoi(argv[1]);
	bool randomSplit=false;
	char* attNum=argv[3];
	char* order=argv[4];
	char* outFile=argv[5];
    char* sorter1=argv[6];
    char* sorter2=argv[7];

    //create strings to invoke sorting executables
    char* prefix="./";
    char* sorter1exe=malloc(strlen(sorter1)+1+4);
    strcpy(sorter1exe,prefix);
    strcat(sorter1exe,sorter1);
    char* sorter2exe=malloc(strlen(sorter2)+1+4);
    strcpy(sorter2exe,prefix);
    strcat(sorter2exe,sorter2);

    //check if random split among workers is needed
    if (strcmp(argv[2],"r")==0){
        randomSplit=true;
    }


    //read number of records in the file
    FILE* fp=fopen(filename,"r");
    if (fp==NULL){
        printf("input file could not be opened\n");
        exit(-1);
    }    
    char* line = NULL;
    size_t nb2read = 0;
    ssize_t nbread;
    int count=0;
    while(nbread=getline(&line,&nb2read,fp)!=-1){
        count++;
    }
    fclose(fp);

    //to store pid of k sorters
    pid_t sorters[k];

    //pipes to pass sorted records and time data respectively
    char* pipeName[k];
    char* timePipe[k];
    
    //create names for pipes for each sorter to pass its sorted records and time data
    //ith pipe is for ith sorter
    int pipeBuffSize=512;
    for (int i=0;i<k;i++){
        // printf("here\n");
        pipeName[i] = malloc(pipeBuffSize*sizeof(char));
        timePipe[i] = malloc(pipeBuffSize*sizeof(char));
        
        strcpy(pipeName[i],"/tmp/pipe");
        strcpy(timePipe[i],"/tmp/timePipe");
        
        char pipeNum[7];
        sprintf(pipeNum,"%d",i);
        strcat(pipeName[i],pipeNum);
        strcat(timePipe[i],pipeNum);
    }
    //create the named pipes
    for (int i=0;i<k;i++){
        mkfifo(pipeName[i],0777);
        mkfifo(timePipe[i],0777);
    }

    //calculate the start and stop index for each of the k workers
    int start[k];
    int stop[k];
    //if not randomly splitting, distribute the range uniformly
    if (!randomSplit){
        for(int i=0;i<k;i++){
            start[i]=i*(count/k);
            if (i!=k-1){
                stop[i]=start[i]+(count/k)-1;  
            }
            else{
                stop[i]=count-1;
            }
        }           
    }
    //if splitting randomly, calculate non-zero ranges for each of the k sorters 
    else{
        int randomizer[k];
        int sum=0;
        //seed for rand()
        srand(time(NULL));
        for (int i=0;i<k;i++){
            randomizer[i]=rand()%k+1;//generate a non zero random number less than k
            sum+=randomizer[i];//compute the sum of all the random numbers
        }
        for (int i=0;i<k;i++){
            //each range is a percentage of the respective random number from the total sum
            //and is multiplied by the count
            randomizer[i]=(randomizer[i]*count)/sum;
        }
        sum=0;
        //assign starting and stopping indices for each of the k workers
        for (int i=0;i<k;i++){
            if (i!=0){
                start[i]=stop[i-1]+1;
            }else{
                start[i]=0;
            }
            if (i!=k-1){
                stop[i]=start[i]+randomizer[i]-1;
                sum+=randomizer[i];
            }else{
                stop[i]=count-1;
                sum+=count-sum;
            }
        }
        

    }
    
    //get process ID of root for sending signals to root
    pid_t rootID=getppid();
	char rootIDstr[20];
	sprintf(rootIDstr,"%d",rootID);

    for (int i=0;i<k;i++){

        //create k sorters
        if (fork()==0){

            //convert range indices for each sorter to char arrays            
            char starting[12];
            sprintf(starting,"%d",start[i]);
            char stopping[12];
            sprintf(stopping,"%d",stop[i]);

            if(i%2){
                //odd numbered sorter

                //load the odd sorter (insertion sort) program with the filename, starting and stopping indices, attribute for comparison,
                //order, the names of pipes for sending sorted records and time data, and the PID of root to send signals
                if(execlp(sorter2exe,filename,starting,stopping,attNum,order,pipeName[i],timePipe[i],rootIDstr,NULL)==-1){
                    int e=errno;
                    printf("ERROR %d\n",e);
                    printf("odd sorter not executing\n");
                };
            }else{
                //even numbered sorter

                //load the even sorter (bubble sort) program with the filename, starting and stopping indices, attribute for comparison,
                //order, the names of pipes for sending sorted records and time data, and the PID of root to send signals
                if(execlp(sorter1exe,filename,starting,stopping,attNum,order,pipeName[i],timePipe[i],rootIDstr,NULL)==-1){
                    int e=errno;
                    printf("ERROR %d\n",e);
                    printf("even sorter not executing\n");
                };

            }
            //exiting sorter
            exit(0);
        }
    }

    //create merger node as a coord-created node
    pid_t mergerID=fork();
    if (mergerID==0){
        //merger node
        //record time stats
        double t1, t2, cpu_time,real_time;
        struct tms tb1, tb2;
        double ticspersec;
        int i, sum = 0;

        ticspersec = (double) sysconf(_SC_CLK_TCK);
        t1 = (double) times(&tb1);

        int fd[k]; //pipe to receive sorted records from sorters
        // int wsp[k]; 
        // int dsp[k];

        int td[k]; //pipe to receive time data
        struct timeData{
            double cpu_time;
            double real_time;
        };
        struct timeData timeStorage[k]; //struct to store time data for each sorter

        


        //use named pipe to get time stats for each sorter
        for (int i=0;i<k;i++){
            td[i]=open(timePipe[i],O_RDONLY);
            read(td[i],&timeStorage[i],sizeof(struct timeData));
            close(td[i]);
        }
        double sumCpu=0;
        double sumReal=0;
        //print time stats for each sorter and sum for total turnaround
        for (int i=0;i<k;i++){
            printf("sorting time for sorter %d: CPU_TIME: %lf SEC, REAL_TIME %lf SEC\n",i,timeStorage[i].cpu_time,timeStorage[i].real_time);
            sumCpu=sumCpu+timeStorage[i].cpu_time;
            sumReal=sumReal+timeStorage[i].real_time;
        }

        //struct to read data from each of the k sorters equal to the range of records it worked on
        struct records* psData[k];
        int range;
        for (int i=0;i<k;i++){
            if (i!=k-1){
                range=start[i+1]-start[i];
            }else{
                range=stop[i]-start[i]+1;
            }
            psData[i]=malloc(range*sizeof(struct records));
        }

        //read data from each of the k sorters equal to the range of records it worked on
        for (int i=0;i<k;i++){
            if (i!=k-1){
                range=start[i+1]-start[i];
            }else{
                range=stop[i]-start[i]+1;
            }
            fd[i]=open(pipeName[i],O_RDONLY);

            for (int j=0;j<range;j++){
                read(fd[i],&(psData[i][j]),sizeof(struct records));
            }
            close(fd[i]);

        }

        //store the partially sorted records in a single array
        struct records sortedArr[count];
        int stored=0;
        for (int i=0;i<k;i++){
            if (i!=k-1){
                range=start[i+1]-start[i];
            }else{
                range=stop[i]-start[i]+1;
            }
            for (int j=0;j<range;j++){
                sortedArr[stored].resID=psData[i][j].resID;
                strncpy(sortedArr[stored].firstName,psData[i][j].firstName,32);
                strncpy(sortedArr[stored].lastName,psData[i][j].lastName,32);
                sortedArr[stored].numDependents=psData[i][j].numDependents;
                sortedArr[stored].income=psData[i][j].income;
                sortedArr[stored].postalCode=psData[i][j].postalCode;
                stored++;
            }
        }


        ///////////////////////////////////////////////////////////////
        /////////SORT THE PARTIALLY SORTED RECORDS USING BUBBLE SORT//
        ////////AND THE RESPECTIVE ORDER AND ATTRIBUTE NUMBER////////
        ////////////////////////////////////////////////////////////
        for (int i=0;i<k;i++){

            for (int j=0;j<k-i-1;j++){

                if (atoi(attNum)==0){

                    if (strcmp(order,"a")==0){
                        if (sortedArr[j].resID>sortedArr[j+1].resID){
                            struct records temp;
                            temp.resID=sortedArr[j].resID;
                            strncpy(temp.firstName,sortedArr[j].firstName,32);
                            strncpy(temp.lastName,sortedArr[j].lastName,32);
                            temp.numDependents=sortedArr[j].numDependents;
                            temp.income=sortedArr[j].income;
                            temp.postalCode=sortedArr[j].postalCode;

                            sortedArr[j].resID=sortedArr[j+1].resID;
                            strncpy(sortedArr[j].firstName,sortedArr[j+1].firstName,32);
                            strncpy(sortedArr[j].lastName,sortedArr[j+1].lastName,32);
                            sortedArr[j].numDependents=sortedArr[j+1].numDependents;
                            sortedArr[j].income=sortedArr[j+1].income;
                            sortedArr[j].postalCode=sortedArr[j+1].postalCode;

                            sortedArr[j+1].resID=temp.resID;
                            strncpy(sortedArr[j+1].firstName,temp.firstName,32);
                            strncpy(sortedArr[j+1].lastName,temp.lastName,32);
                            sortedArr[j+1].numDependents=temp.numDependents;
                            sortedArr[j+1].income=temp.income;
                            sortedArr[j+1].postalCode=temp.postalCode;
                        }
                    }
                    else if (strcmp(order,"d")==0){
                        if (sortedArr[j].resID<sortedArr[j+1].resID){
                            struct records temp;
                            temp.resID=sortedArr[j].resID;
                            strncpy(temp.firstName,sortedArr[j].firstName,32);
                            strncpy(temp.lastName,sortedArr[j].lastName,32);
                            temp.numDependents=sortedArr[j].numDependents;
                            temp.income=sortedArr[j].income;
                            temp.postalCode=sortedArr[j].postalCode;

                            sortedArr[j].resID=sortedArr[j+1].resID;
                            strncpy(sortedArr[j].firstName,sortedArr[j+1].firstName,32);
                            strncpy(sortedArr[j].lastName,sortedArr[j+1].lastName,32);
                            sortedArr[j].numDependents=sortedArr[j+1].numDependents;
                            sortedArr[j].income=sortedArr[j+1].income;
                            sortedArr[j].postalCode=sortedArr[j+1].postalCode;

                            sortedArr[j+1].resID=temp.resID;
                            strncpy(sortedArr[j+1].firstName,temp.firstName,32);
                            strncpy(sortedArr[j+1].lastName,temp.lastName,32);
                            sortedArr[j+1].numDependents=temp.numDependents;
                            sortedArr[j+1].income=temp.income;
                            sortedArr[j+1].postalCode=temp.postalCode;
                        }
                    }
                }
                else if (atoi(attNum)==3){
                    if (strcmp(order,"a")==0){
                        if (sortedArr[j].numDependents>sortedArr[j+1].numDependents){
                            struct records temp;
                            temp.resID=sortedArr[j].resID;
                            strncpy(temp.firstName,sortedArr[j].firstName,32);
                            strncpy(temp.lastName,sortedArr[j].lastName,32);
                            temp.numDependents=sortedArr[j].numDependents;
                            temp.income=sortedArr[j].income;
                            temp.postalCode=sortedArr[j].postalCode;

                            sortedArr[j].resID=sortedArr[j+1].resID;
                            strncpy(sortedArr[j].firstName,sortedArr[j+1].firstName,32);
                            strncpy(sortedArr[j].lastName,sortedArr[j+1].lastName,32);
                            sortedArr[j].numDependents=sortedArr[j+1].numDependents;
                            sortedArr[j].income=sortedArr[j+1].income;
                            sortedArr[j].postalCode=sortedArr[j+1].postalCode;

                            sortedArr[j+1].resID=temp.resID;
                            strncpy(sortedArr[j+1].firstName,temp.firstName,32);
                            strncpy(sortedArr[j+1].lastName,temp.lastName,32);
                            sortedArr[j+1].numDependents=temp.numDependents;
                            sortedArr[j+1].income=temp.income;
                            sortedArr[j+1].postalCode=temp.postalCode;
                        }
                    }
                    else if (strcmp(order,"d")==0){
                        if (sortedArr[j].numDependents<sortedArr[j+1].numDependents){
                            struct records temp;
                            temp.resID=sortedArr[j].resID;
                            strncpy(temp.firstName,sortedArr[j].firstName,32);
                            strncpy(temp.lastName,sortedArr[j].lastName,32);
                            temp.numDependents=sortedArr[j].numDependents;
                            temp.income=sortedArr[j].income;
                            temp.postalCode=sortedArr[j].postalCode;

                            sortedArr[j].resID=sortedArr[j+1].resID;
                            strncpy(sortedArr[j].firstName,sortedArr[j+1].firstName,32);
                            strncpy(sortedArr[j].lastName,sortedArr[j+1].lastName,32);
                            sortedArr[j].numDependents=sortedArr[j+1].numDependents;
                            sortedArr[j].income=sortedArr[j+1].income;
                            sortedArr[j].postalCode=sortedArr[j+1].postalCode;

                            sortedArr[j+1].resID=temp.resID;
                            strncpy(sortedArr[j+1].firstName,temp.firstName,32);
                            strncpy(sortedArr[j+1].lastName,temp.lastName,32);
                            sortedArr[j+1].numDependents=temp.numDependents;
                            sortedArr[j+1].income=temp.income;
                            sortedArr[j+1].postalCode=temp.postalCode;
                        }
                    }
                }
                else if (atoi(attNum)==4){
                    if (strcmp(order,"a")==0){
                        if (sortedArr[j].income>sortedArr[j+1].income){
                            struct records temp;
                            temp.resID=sortedArr[j].resID;
                            strncpy(temp.firstName,sortedArr[j].firstName,32);
                            strncpy(temp.lastName,sortedArr[j].lastName,32);
                            temp.numDependents=sortedArr[j].numDependents;
                            temp.income=sortedArr[j].income;
                            temp.postalCode=sortedArr[j].postalCode;

                            sortedArr[j].resID=sortedArr[j+1].resID;
                            strncpy(sortedArr[j].firstName,sortedArr[j+1].firstName,32);
                            strncpy(sortedArr[j].lastName,sortedArr[j+1].lastName,32);
                            sortedArr[j].numDependents=sortedArr[j+1].numDependents;
                            sortedArr[j].income=sortedArr[j+1].income;
                            sortedArr[j].postalCode=sortedArr[j+1].postalCode;

                            sortedArr[j+1].resID=temp.resID;
                            strncpy(sortedArr[j+1].firstName,temp.firstName,32);
                            strncpy(sortedArr[j+1].lastName,temp.lastName,32);
                            sortedArr[j+1].numDependents=temp.numDependents;
                            sortedArr[j+1].income=temp.income;
                            sortedArr[j+1].postalCode=temp.postalCode;
                        }
                    }
                    else if (strcmp(order,"d")==0){
                        if (sortedArr[j].income<sortedArr[j+1].income){
                            struct records temp;
                            temp.resID=sortedArr[j].resID;
                            strncpy(temp.firstName,sortedArr[j].firstName,32);
                            strncpy(temp.lastName,sortedArr[j].lastName,32);
                            temp.numDependents=sortedArr[j].numDependents;
                            temp.income=sortedArr[j].income;
                            temp.postalCode=sortedArr[j].postalCode;

                            sortedArr[j].resID=sortedArr[j+1].resID;
                            strncpy(sortedArr[j].firstName,sortedArr[j+1].firstName,32);
                            strncpy(sortedArr[j].lastName,sortedArr[j+1].lastName,32);
                            sortedArr[j].numDependents=sortedArr[j+1].numDependents;
                            sortedArr[j].income=sortedArr[j+1].income;
                            sortedArr[j].postalCode=sortedArr[j+1].postalCode;

                            sortedArr[j+1].resID=temp.resID;
                            strncpy(sortedArr[j+1].firstName,temp.firstName,32);
                            strncpy(sortedArr[j+1].lastName,temp.lastName,32);
                            sortedArr[j+1].numDependents=temp.numDependents;
                            sortedArr[j+1].income=temp.income;
                            sortedArr[j+1].postalCode=temp.postalCode;
                        }
                    }
                }
                else if (atoi(attNum)==5){
                    if (strcmp(order,"a")==0){
                        if (sortedArr[j].postalCode>sortedArr[j+1].postalCode){
                            struct records temp;
                            temp.resID=sortedArr[j].resID;
                            strncpy(temp.firstName,sortedArr[j].firstName,32);
                            strncpy(temp.lastName,sortedArr[j].lastName,32);
                            temp.numDependents=sortedArr[j].numDependents;
                            temp.income=sortedArr[j].income;
                            temp.postalCode=sortedArr[j].postalCode;

                            sortedArr[j].resID=sortedArr[j+1].resID;
                            strncpy(sortedArr[j].firstName,sortedArr[j+1].firstName,32);
                            strncpy(sortedArr[j].lastName,sortedArr[j+1].lastName,32);
                            sortedArr[j].numDependents=sortedArr[j+1].numDependents;
                            sortedArr[j].income=sortedArr[j+1].income;
                            sortedArr[j].postalCode=sortedArr[j+1].postalCode;

                            sortedArr[j+1].resID=temp.resID;
                            strncpy(sortedArr[j+1].firstName,temp.firstName,32);
                            strncpy(sortedArr[j+1].lastName,temp.lastName,32);
                            sortedArr[j+1].numDependents=temp.numDependents;
                            sortedArr[j+1].income=temp.income;
                            sortedArr[j+1].postalCode=temp.postalCode;
                        }
                    }
                    else if (strcmp(order,"d")==0){
                        if (sortedArr[j].postalCode<sortedArr[j+1].postalCode){
                            struct records temp;
                            temp.resID=sortedArr[j].resID;
                            strncpy(temp.firstName,sortedArr[j].firstName,32);
                            strncpy(temp.lastName,sortedArr[j].lastName,32);
                            temp.numDependents=sortedArr[j].numDependents;
                            temp.income=sortedArr[j].income;
                            temp.postalCode=sortedArr[j].postalCode;

                            sortedArr[j].resID=sortedArr[j+1].resID;
                            strncpy(sortedArr[j].firstName,sortedArr[j+1].firstName,32);
                            strncpy(sortedArr[j].lastName,sortedArr[j+1].lastName,32);
                            sortedArr[j].numDependents=sortedArr[j+1].numDependents;
                            sortedArr[j].income=sortedArr[j+1].income;
                            sortedArr[j].postalCode=sortedArr[j+1].postalCode;

                            sortedArr[j+1].resID=temp.resID;
                            strncpy(sortedArr[j+1].firstName,temp.firstName,32);
                            strncpy(sortedArr[j+1].lastName,temp.lastName,32);
                            sortedArr[j+1].numDependents=temp.numDependents;
                            sortedArr[j+1].income=temp.income;
                            sortedArr[j+1].postalCode=temp.postalCode;
                        }
                    }
                }
            }
        }

        //store time data for end of merger
        t2 = (double) times(&tb2);
        cpu_time = (double) ((tb2.tms_utime + tb2.tms_stime) -(tb1.tms_utime + tb1.tms_stime));
        cpu_time/=ticspersec;
        real_time=(t2 - t1) / ticspersec;
        sumCpu=sumCpu+cpu_time;
        sumReal=sumReal+real_time;

        //print time stats
        printf("Time taken by merger CPU_TIME: %lf SEC, REAL_TIME %lf SEC\n",cpu_time,real_time);
        printf("Turnaround time for sorting CPU_TIME: %lf SEC, REAL_TIME %lf SEC\n",sumCpu,sumReal);

        
        //output sorted data to file
        FILE *fout;
        if (fout==NULL){
            printf("FILE WRITING FAILED\n");
        }
        printf("\nPLEASE WAIT FOR WRITING TO THE FILE\n");
        fout=fopen(outFile,"w");
        close(fout);
        for (int i=0;i<count;i++){

            fout=fopen(outFile,"a");

            fprintf(fout,"%d",sortedArr[i].resID);
            fclose(fout);

            fout=fopen(outFile,"a");

            fprintf(fout,",%s",sortedArr[i].firstName);
            fclose(fout);

            fout=fopen(outFile,"a");

            fprintf(fout,",%s",sortedArr[i].lastName);
            fclose(fout);

            fout=fopen(outFile,"a");

            fprintf(fout,",%d",sortedArr[i].numDependents);
            fclose(fout);

            fout=fopen(outFile,"a");

            fprintf(fout,",%0.2f",sortedArr[i].income);
            fclose(fout);

            fout=fopen(outFile,"a");

            fprintf(fout,",%d\n",sortedArr[i].postalCode);
            fclose(fout);
        }
        //send SIGUSR2 signal from merger to root
        kill(rootID,SIGUSR2);
        exit(0);

    }
    else{
        //coordinator node
        //wait for all children of coord to exit
        while (wait(NULL)!=-1){
            continue;
        }
        exit(0);
    }

    return 0;
}