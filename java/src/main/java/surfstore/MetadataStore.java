package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;
	protected static int number;
	protected static int leader;
    private final ManagedChannel blockChannel;
    private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
    private final ManagedChannel [] metaChannel;
    protected static MetadataStoreGrpc.MetadataStoreBlockingStub [] followers;
    private final ManagedChannel leaderChannel;
    protected static MetadataStoreGrpc.MetadataStoreBlockingStub leaderStub;
    protected static boolean crashed;
    protected static int metaNumbers;
    protected  static List<List<String>> log;     //Operation + filename + blocks

    public MetadataStore(ConfigReader config, int number, int leader, int metaNumbers) { //status is 1 if centralized, >1 if distributed
    	this.config = config;
        this.number = number;
        this.leader = leader;
        this.crashed = false;
        this.metaNumbers = metaNumbers;
    	if(number == leader) {
            this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                    .usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
        }
        else{
    	    this.blockChannel = null;
    	    this.blockStub = null;
        }
        if(number == leader && metaNumbers>1){
            this.metaChannel = new ManagedChannel[2];
            this.followers = new MetadataStoreGrpc.MetadataStoreBlockingStub[2];
            this.leaderChannel = null;
            leaderStub = null;
    	    for(int i=0;i<2;i++){
    	       this.metaChannel[i] = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i+2))
                       .usePlaintext(true).build();
    	       this.followers[i] = MetadataStoreGrpc.newBlockingStub(this.metaChannel[i]);
    	       leaderStub = null;
            }
        }
        else if(metaNumbers>1){
            this.metaChannel = null;
            this.followers = null;
            this.leaderChannel = ManagedChannelBuilder.forAddress("127.0.0.1",config.getMetadataPort(number))
                    .usePlaintext(true).build();
            this.leaderStub = MetadataStoreGrpc.newBlockingStub(this.leaderChannel);
        }
        else {
    	    this.metaChannel = null;
            this.followers = null;
            this.leaderChannel = null;
            this.leaderStub = null;
        }
        if(metaNumbers>1){
    	    log = new LinkedList<>();
        }
        else{
    	    log = null;
        }

	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
    private void update(){
        for(int i=0;i<followers.length;i++){
            Empty empty = Empty.newBuilder().build();
            if(metaChannel[i].getState(true)==ConnectivityState.READY&&!followers[i].isCrashed(empty).getAnswer()){
                int index = followers[i].readIndex(empty).getIndex();
                while(index<log.size()&&!followers[i].isCrashed(empty).getAnswer()) {
                    List<String> operation = log.get(index);
                    String action = operation.get(0);
                    logger.info(action);
                    String filename = operation.get(1);
                    int version = Integer.parseInt(operation.get(2));
                    FileInfo.Builder fileBuilder = FileInfo.newBuilder();
                    fileBuilder.setVersion(version);
                    fileBuilder.setFilename(filename);
                    EventInfo.Builder eventBuilder = EventInfo.newBuilder();
                    eventBuilder.setIndex(index);
                    if (action.equals("Delete")) {
                        FileInfo fileInfo = fileBuilder.build();
                        eventBuilder.setFileInfo(fileInfo);
                        EventInfo eventInfo = eventBuilder.build();
                        followers[i].deleteInFollow(eventInfo);
                    }
                    else if(action.equals("Modify")){
                        List<String> blocks = new ArrayList<>();
                        for(int j=3;j<operation.size();j++){
                            blocks.add(operation.get(j));
                        }
                        fileBuilder.addAllBlocklist(blocks);
                        FileInfo fileInfo = fileBuilder.build();
                        eventBuilder.setFileInfo(fileInfo);
                        EventInfo eventInfo = eventBuilder.build();
                        followers[i].modifyFollow(eventInfo);
                    }
                    index++;
                }
            }
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }
        int number = c_args.getInt("number");
        int leader = config.leaderNum;
        final MetadataStore server = new MetadataStore(config, number, leader, config.getNumMetadataServers());
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        Thread thread = new Thread(){
            public synchronized void run() {
                while (true) {
                    server.update();
                    try {
                        wait(500);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        if(metaNumbers>1&&number==leader) {
            thread.start();
        }

        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        HashMap<String, Integer> versionMap;
        HashMap<String, List<String>> hashlist;
        List<String> rollback;

        public MetadataStoreImpl() {
            this.versionMap = new HashMap<>();
            this.hashlist = new HashMap<>();
            rollback = new ArrayList<>();
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        public synchronized void readFile(surfstore.SurfStoreBasic.FileInfo request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            String filename = request.getFilename();
            int version;
            FileInfo.Builder builder = FileInfo.newBuilder();
            if (versionMap.containsKey(filename)) {
                version = versionMap.get(filename);
                List<String> list = hashlist.get(filename);
                builder.setVersion(version);
                builder.addAllBlocklist(list);
                builder.setFilename(filename);
            } else {
                builder.setFilename(filename);
                builder.setVersion(0);
            }

            FileInfo fileInfo = builder.build();
            responseObserver.onNext(fileInfo);
            responseObserver.onCompleted();
        }

        public synchronized void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            int newversion = request.getVersion();
            String filename = request.getFilename();
            int oldversion = 0;
            oldversion = versionMap.getOrDefault(filename, 0);
            int missblocknum = 0;
            WriteResult.Builder resultbuilder = WriteResult.newBuilder();
            if (number != leader) {
                resultbuilder.setResult(WriteResult.Result.NOT_LEADER);
                WriteResult result = resultbuilder.build();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } else if (newversion != oldversion + 1) {
                resultbuilder.setResult(WriteResult.Result.OLD_VERSION);
                resultbuilder.setCurrentVersion(oldversion);
                WriteResult result = resultbuilder.build();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } else if(metaNumbers>1) {
                int index = log.size();
                EventIndex eventIndex = EventIndex.newBuilder().setIndex(index).build();
                EventInfo.Builder eventInfoBuilder = EventInfo.newBuilder();
                eventInfoBuilder.setFileInfo(request);
                eventInfoBuilder.setIndex(index);
                EventInfo eventInfo = eventInfoBuilder.build();
                int voted = 1;
                for (int i = 0; i < followers.length; i++) {
                    if (followers[i].modifyFollow(eventInfo).getResult()==WriteResult.Result.OK) {
                        voted++;
                    }
                }
                if (voted > metaNumbers / 2) {
                    List<String> missblock = new ArrayList<>();
                    List<String> fileblock = new ArrayList<>();
                    for (int i = 0; i < request.getBlocklistCount(); i++) {
                        String blockhash = request.getBlocklist(i);
                        fileblock.add(blockhash);
                        Block.Builder blockbuilder = Block.newBuilder();
                        blockbuilder.setHash(blockhash);
                        Block block = blockbuilder.build();
                        if (!blockStub.hasBlock(block).getAnswer()) {
                            missblocknum++;
                            missblock.add(blockhash);
                        }
                    }
                    SimpleAnswer.Builder answerBuilder = SimpleAnswer.newBuilder();
                    if (missblocknum == 0) {
                        resultbuilder.setResult(WriteResult.Result.OK);
                        resultbuilder.setCurrentVersion(newversion);
                        versionMap.put(filename, newversion);
                        hashlist.put(filename, fileblock);
                        List<String> commit = new ArrayList<>();
                        commit.add("Modify");
                        commit.add(filename);
                        commit.add(String.valueOf(newversion));
                        for (int i = 0; i < fileblock.size(); i++) {
                            commit.add(fileblock.get(i));
                        }
                        log.add(commit);
                        WriteResult result = resultbuilder.build();
                        responseObserver.onNext(result);
                        responseObserver.onCompleted();
                        for (int i = 0; i < followers.length; i++) {
                            answerBuilder.setAnswer(true);
                            SimpleAnswer answer = answerBuilder.build();
                            followers[i].notifyResult(answer);
                        }
                    } else {
                        resultbuilder.setResult(WriteResult.Result.MISSING_BLOCKS);
                        resultbuilder.addAllMissingBlocks(missblock);
                        resultbuilder.setCurrentVersion(oldversion);
                        answerBuilder.setAnswer(false);
                        SimpleAnswer answer = answerBuilder.build();
                        for(int i=0;i<followers.length;i++) {
                            followers[i].notifyResult(answer);
                        }
                        WriteResult result = resultbuilder.build();
                        responseObserver.onNext(result);
                        responseObserver.onCompleted();
                    }

                } else {
                    SimpleAnswer.Builder answerBuilder = SimpleAnswer.newBuilder();
                    resultbuilder.setResult(WriteResult.Result.CRASHED);
                    answerBuilder.setAnswer(false);
                    SimpleAnswer answer = answerBuilder.build();
                    for(int i=0;i<followers.length;i++) {
                        followers[i].notifyResult(answer);
                    }
                    WriteResult result = resultbuilder.build();
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
                }
            }
            else{
                List<String> missblock = new ArrayList<>();
                List<String> fileblock = new ArrayList<>();
                for (int i = 0; i < request.getBlocklistCount(); i++) {
                    String blockhash = request.getBlocklist(i);
                    fileblock.add(blockhash);
                    Block.Builder blockbuilder = Block.newBuilder();
                    blockbuilder.setHash(blockhash);
                    Block block = blockbuilder.build();
                    if (!blockStub.hasBlock(block).getAnswer()) {
                        missblocknum++;
                        missblock.add(blockhash);
                    }
                }
                if (missblocknum == 0) {
                    resultbuilder.setResult(WriteResult.Result.OK);
                    resultbuilder.setCurrentVersion(newversion);
                    versionMap.put(filename, newversion);
                    hashlist.put(filename, fileblock);
                } else {
                    resultbuilder.setResult(WriteResult.Result.MISSING_BLOCKS);
                    resultbuilder.addAllMissingBlocks(missblock);
                    resultbuilder.setCurrentVersion(oldversion);
                }
                WriteResult result = resultbuilder.build();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }

        }


        public synchronized void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            String filename = request.getFilename();
            int newversion = request.getVersion();
            int oldversion = versionMap.getOrDefault(filename, 0);
            SimpleAnswer.Builder answerBuilder = SimpleAnswer.newBuilder();
            WriteResult.Builder resultbuilder = WriteResult.newBuilder();
            WriteResult result;
            if (leader != number) {
                resultbuilder.setResult(WriteResult.Result.NOT_LEADER);
                result = resultbuilder.build();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }
            else if (oldversion == 0 || newversion != oldversion + 1) {
                    resultbuilder.setResult(WriteResult.Result.OLD_VERSION);
                    result = resultbuilder.build();
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
            }
            else if(metaNumbers>1) {
                int voted = 1;
                int index = log.size();
                for(int i=0;i<followers.length;i++){
                    EventInfo.Builder eventBuilder = EventInfo.newBuilder();
                    eventBuilder.setIndex(index);
                    eventBuilder.setFileInfo(request);
                    EventInfo deleteInfo = eventBuilder.build();
                    if(followers[i].deleteInFollow(deleteInfo).getResult()==WriteResult.Result.OK){
                        voted++;
                    }
                }
                if(voted > metaNumbers/2) {
                    List<String> list = new ArrayList<>();
                    list.add(new String("0"));
                    hashlist.put(filename, list);
                    versionMap.put(filename, newversion);
                    List<String> commit = new ArrayList<>();
                    commit.add("Delete");
                    commit.add(filename);
                    commit.add(String.valueOf(newversion));
                    log.add(commit);
                    answerBuilder.setAnswer(true);
                    SimpleAnswer answer = answerBuilder.build();
                    resultbuilder.setResult(WriteResult.Result.OK);
                    resultbuilder.setCurrentVersion(newversion);
                    result = resultbuilder.build();
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
                    for(int i=0;i<followers.length;i++){
                        followers[i].notifyResult(answer);
                    }
                }
                else{
                    answerBuilder.setAnswer(false);
                    SimpleAnswer answer = answerBuilder.build();
                    resultbuilder.setResult(WriteResult.Result.CRASHED);
                    result = resultbuilder.build();
                    for(int i=0;i<followers.length;i++){
                        followers[i].notifyResult(answer);
                    }
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
                }
            }
            else{
                List<String> list = new ArrayList<>();
                list.add(new String("0"));
                hashlist.put(filename, list);
                versionMap.put(filename, newversion);
                resultbuilder.setResult(WriteResult.Result.OK);
                resultbuilder.setCurrentVersion(newversion);
                result = resultbuilder.build();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }

        }

        public synchronized void isLeader(surfstore.SurfStoreBasic.Empty request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer answer = SimpleAnswer.newBuilder().setAnswer(number==leader).build();
            responseObserver.onNext(answer);
            responseObserver.onCompleted();
        }

        public synchronized void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            crashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        public synchronized void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            crashed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        public synchronized void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer answer = SimpleAnswer.newBuilder().setAnswer(crashed).build();
            responseObserver.onNext(answer);
            responseObserver.onCompleted();
        }

        public synchronized void getVersion(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            FileInfo.Builder builder = FileInfo.newBuilder();
            String filename = request.getFilename();
            builder.setFilename(filename);
            int currentVersion = versionMap.get(filename);
            builder.setVersion(currentVersion);
            FileInfo fileInfo = builder.build();
            responseObserver.onNext(fileInfo);
            responseObserver.onCompleted();
        }
        public synchronized void modifyFollow(surfstore.SurfStoreBasic.EventInfo request,
                                 io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            WriteResult.Builder resultBuilder = WriteResult.newBuilder();
            FileInfo fileInfo = request.getFileInfo();
            if(crashed || request.getIndex()!=log.size()){
                resultBuilder.setResult(WriteResult.Result.CRASHED);
            }
            else{
                int version = fileInfo.getVersion();
                String filename = fileInfo.getFilename();
                int oldversion = versionMap.getOrDefault(filename,0);
                versionMap.put(filename,version);
                List<String> blocklist = new ArrayList<>();
                List<String> commit = new ArrayList<>();
                commit.add("Modify");
                commit.add(filename);
                commit.add(String.valueOf(version));
                for(int i=0;i<fileInfo.getBlocklistCount();i++){
                    blocklist.add(fileInfo.getBlocklist(i));
                    logger.info("Modify follow " + fileInfo.getBlocklist(i));
                    commit.add(fileInfo.getBlocklist(i));
                }
                rollback.add(filename);
                rollback.add(String.valueOf(oldversion));
                if(oldversion!=0) {
                    for (int i = 0; i < hashlist.get(filename).size(); i++) {
                        rollback.add(hashlist.get(filename).get(i));
                    }
                }
                hashlist.put(filename,blocklist);
                log.add(commit);
                logger.info("follower got modified");
                resultBuilder.setResult(WriteResult.Result.OK);
            }
            WriteResult result = resultBuilder.build();
            responseObserver.onNext(result);
            responseObserver.onCompleted();
;
        }

        public synchronized void deleteInFollow(surfstore.SurfStoreBasic.EventInfo request,
                                   io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            WriteResult.Builder resultBuilder = WriteResult.newBuilder();
            FileInfo fileInfo = request.getFileInfo();
            if(crashed || request.getIndex()!=log.size()){
                resultBuilder.setResult(WriteResult.Result.CRASHED);
            }
            else{
                int version = fileInfo.getVersion();
                String filename = fileInfo.getFilename();
                rollback.add(filename);
                int oldversion = versionMap.getOrDefault(filename,0);
                rollback.add(String.valueOf(oldversion));
                if(oldversion!=0){
                    for(int i=0;i<hashlist.get(filename).size();i++){
                        rollback.add(hashlist.get(filename).get(i));
                    }
                }
                versionMap.put(filename,version);
                List<String> block = new ArrayList<>();
                block.add("0");
                hashlist.put(filename,block);
                List<String> commit = new ArrayList<>();
                commit.add("Delete");
                commit.add(filename);
                commit.add(String.valueOf(version));
                log.add(commit);
                logger.info("follow got deleted");
                resultBuilder.setResult(WriteResult.Result.OK);
            }
            WriteResult result = resultBuilder.build();
            responseObserver.onNext(result);
            responseObserver.onCompleted();

        }

        public synchronized void readIndex(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.EventIndex> responseObserver) {
            int index = log.size();
            EventIndex eventIndex = EventIndex.newBuilder().setIndex(index).build();
            responseObserver.onNext(eventIndex);
            responseObserver.onCompleted();

        }
        public void notifyResult(surfstore.SurfStoreBasic.SimpleAnswer request,
                                 io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            boolean answer = request.getAnswer();
            if(!answer){
                logger.info("rollback size is" + String.valueOf(rollback.size()));
                int version = Integer.parseInt(rollback.get(1));
                String filename = rollback.get(0);
                if(version==0){
                    versionMap.remove(filename);
                    hashlist.remove(filename);
                }
                else{
                    versionMap.put(filename,version);
                    List<String> hash = new ArrayList<>();
                    for(int i=2;i<rollback.size();i++){
                        hash.add(rollback.get(i));
                    }
                    hashlist.put(filename,hash);
                }
                rollback.clear();
                log.remove(log.size()-1);
            }
            else{
                rollback.clear();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}