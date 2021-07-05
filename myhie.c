#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/times.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>


//handler for SIGUSR1
int numSig1 = 0;
void sig1handler(int sig) {
    signal(SIGUSR1, sig1handler);
    numSig1++;
}

//handler for SIGUSR2
int numSig2 = 0;
void sig2handler(int sig) {
    signal(SIGUSR2, sig2handler);
    numSig2++;
}

int main(int argc, char* argv[]) {
	//for signal handling from the subsequent nodes
	signal(SIGUSR1, sig1handler);
	signal(SIGUSR2, sig2handler);

	//for time calculation of root
	double t1, t2, cpu_time,real_time;
	struct tms tb1, tb2;
	double ticspersec;
	int i, sum = 0;
	//store starting time
	ticspersec = (double) sysconf(_SC_CLK_TCK);
	t1 = (double) times(&tb1);

	//status for waiting for coord by root
	int status;

	//read arguments for invocation
	char* filename;
	char* k;
	bool randomSplit=false;
	char* attNum;
	char* order;
	char* outFile;
	for (int i=1;i<argc;i++){
		if (strcmp(argv[i],"-i")==0){
			filename=argv[i+1];
		}
		else if(strcmp(argv[i],"-k")==0){
			k=argv[i+1];
		}
		else if (strcmp(argv[i],"-r")==0)
		{
			randomSplit=true;
		}
		else if (strcmp(argv[i],"-a")==0)
		{
			attNum=argv[i+1];
		}
		else if (strcmp(argv[i],"-o")==0)
		{
			order=argv[i+1];
		}else if (strcmp(argv[i],"-s")==0)
		{
			outFile=argv[i+1];
		}
		
	}

	
	
	//create coordinate node and store its ID
	int coordID=fork();

	//check for fork failed
	if (coordID<0){
		printf("coord node could not be created by fork\n");
		return -1;
	}

	

	//coord node
	else if(coordID==0){
		//exec to create coord
		//if random split, pass random flag "r"
		if (randomSplit){
			//exec with filename, num of workers, random flag, attribute number for sorting, order of sorting
			//names of executables for sorting
			if(execlp("./coord",filename,k,"r",attNum,order,outFile,"bubble","insertion",NULL)==-1){
				printf("exec not working for creating sorter node\n");
			};
		}
		//if not random split, pass flag "nr"
		else{
			//exec with filename, num of workers,non random flag, attribute number for sorting, order of sorting
			//names of executables for sorting
			if(execlp("./coord",filename,k,"nr",attNum,order,outFile,"bubble","insertion",NULL)==-1){
				printf("exec not working for creating sorter node\n");
			};
		}
		
		
	}

	//root node
	else{
		
		//wait for coord node to exit and check for proper exit
		if ( wait(&status)!=  coordID ) { perror("wait error"); exit (1);}

		//store end time of root
		t2 = (double) times(&tb2);

		//calculate timing statistics
        cpu_time = (double) ((tb2.tms_utime + tb2.tms_stime) -(tb1.tms_utime + tb1.tms_stime));
        cpu_time/=ticspersec;
        real_time=(t2 - t1) / ticspersec;

		//print timing stats
        printf("Total time taken by the root: CPU_TIME: %lf SEC, REAL_TIME %lf SEC\n",cpu_time,real_time);
		//print number of different signals caught
		printf("Number of SIGUSR1 caught: %d\n",numSig1);
		printf("Number of SIGUSR2 caught: %d\n",numSig2);	
		
	}
	exit(0);
}