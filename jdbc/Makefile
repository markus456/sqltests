#CLASSPATH='.:mariadb-java-client-2.5.3.jar'
#CLASSPATH='.:mariadb-java-client-3.1.4.jar'
CLASSPATH='.:mariadb-java-client-3.1.2.jar'
all: build

build: com/mycompany/app/App.class

com/mycompany/app/App.class: com/mycompany/app/App.java
	javac com/mycompany/app/App.java


test: build
	java -cp $(CLASSPATH) com.mycompany.app.App

clean:
	rm -f com/mycompany/app/App.class
