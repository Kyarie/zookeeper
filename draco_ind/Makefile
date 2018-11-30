GS = -g
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
        Main.java \
        DracoRequestProcessor.java \
        OpCode.java \
        Request.java

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
