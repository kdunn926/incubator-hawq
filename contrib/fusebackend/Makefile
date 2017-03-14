all: libs3fs.so

libs3fs.so: libs3fs.cpp
	$(CXX) -I/usr/local/hawq-2.0.0.0/include -I/usr/local/hawq-2.0.0.0/include/postgresql/internal -I/usr/local/hawq-2.0.0.0/include/hdfs -I/usr/local/hawq-2.0.0.0/include/postgresql/server -fPIC -c libs3fs.cpp -o libs3fs.o
	$(CXX) -shared  -Wl,-soname,libs3fs.so -o libs3fs.so libs3fs.o

clean:
	rm *.o *.so

