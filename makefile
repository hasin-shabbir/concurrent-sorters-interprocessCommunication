all: myhie coord bubble insertion

myhie: myhie.c
	gcc myhie.c -o myhie

coord: coord.c
	gcc coord.c -o coord

bubble: bubble.c
	gcc bubble.c -o bubble

insertion: insertion.c
	gcc insertion.c -o insertion

clean:
	rm -f myhie coord bubble insertion *.o