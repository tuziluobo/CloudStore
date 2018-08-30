package surfstore;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    private  ManagedChannel [] followerChannel;
    private  MetadataStoreGrpc.MetadataStoreBlockingStub [] followers;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
        if(config.getNumMetadataServers()>1){
            followers = new MetadataStoreGrpc.MetadataStoreBlockingStub[2];
            followerChannel = new ManagedChannel[2];
            followerChannel[0] = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2))
                    .usePlaintext(true).build();
            followerChannel[1] = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3))
                    .usePlaintext(true).build();
            followers[0] = MetadataStoreGrpc.newBlockingStub(followerChannel[0]);
            followers[1] = MetadataStoreGrpc.newBlockingStub(followerChannel[1]);
        }
        else{
            followers = null;
            followerChannel = null;
        }
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    private void ensure(boolean b){
        if(b == false){
            throw new RuntimeException("Assertion failed!");
        }
    }


     private static Block stringToBlock(String s){
         Block.Builder builder = Block.newBuilder();

         try{
             builder.setData(ByteString.copyFrom(s,"UTF-8"));
         }catch(UnsupportedEncodingException e){
             throw new RuntimeException(e);
         }

         builder.setHash(HashUtils.sha256(s));

         return builder.build();
     }
     private static Block byteToBlock(byte [] s){
         Block.Builder builder = Block.newBuilder();

         //try{
         builder.setData(ByteString.copyFrom(s));
         //}catch(UnsupportedEncodingException e){
           //  throw new RuntimeException(e);
         //}

         builder.setHash(HashUtils.sha256byte(s));

         return builder.build();
     }
     private boolean exist(FileInfo fileInfo){
        if(fileInfo.getVersion()==0)
            return false;
        else if(fileInfo.getBlocklistCount()==1&&fileInfo.getBlocklist(0).equals("0"))
            return false;
        return true;
     }
     private void upload(String localfile){
        File f = new File(localfile);
        String [] path = localfile.split("/");
        String filename = path[path.length-1];
        if(!f.exists()){
            System.out.println("NOT FOUND");
            return;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream((int)f.length());
        BufferedInputStream in = null;
        List<Block> fileblock = new ArrayList<>();
        List<String> hashlist = new ArrayList<>();
        Map<String, Block> hashToblock = new HashMap<>();
        try {
             in = new BufferedInputStream(new FileInputStream(f));
             int blk_size = 4096;
             byte[] block = new byte[blk_size];
             int len = 0;
             Block subfile;
             long remained = f.length();
             while (remained>=blk_size&&-1!= (len = in.read(block, 0, blk_size))) {
                 bos.write(block, 0, len);
                 subfile = byteToBlock(block);
                 fileblock.add(subfile);
                 hashlist.add(subfile.getHash());
                 hashToblock.put(subfile.getHash(),subfile);
                 remained -= blk_size;
             }
             byte [] lastblock = new byte[(int)remained];
             len = in.read(lastblock,0,(int)remained);
             bos.write(lastblock,0,len);
             subfile = byteToBlock(lastblock);
             fileblock.add(subfile);
             hashlist.add(subfile.getHash());
             hashToblock.put(subfile.getHash(),subfile);
             bos.close();
             in.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        //FileInfo readinfo = FileInfo.newBuilder().setFilename(filename).build();
        int oldversion = getVersion(filename);
        logger.info("existing file version is" +String.valueOf(oldversion));
        int newversion = oldversion+1;
        FileInfo.Builder filebuilder = FileInfo.newBuilder();
        filebuilder.setFilename(filename);
        filebuilder.setVersion(newversion);
        filebuilder.addAllBlocklist(hashlist);
        FileInfo fileinfo = filebuilder.build();
        WriteResult result = metadataStub.modifyFile(fileinfo);
        while(result.getResult()!=WriteResult.Result.OK){
            if(result.getResult()==WriteResult.Result.MISSING_BLOCKS){
                logger.info("Missing blocks");
                for(int i=0;i<result.getMissingBlocksCount();i++){
                    String missinghash = result.getMissingBlocks(i);
                    Block missingblock = hashToblock.get(missinghash);
                    blockStub.storeBlock(missingblock);
                }
            }
            else if(result.getResult()==WriteResult.Result.OLD_VERSION) {
                //oldversion = metadataStub.readFile(readinfo).getVersion();
                oldversion = result.getCurrentVersion();
                newversion = oldversion + 1;
                filebuilder.setVersion(newversion);
                fileinfo = filebuilder.build();
            }
            result = metadataStub.modifyFile(fileinfo);
        }
        System.out.println("OK");
     }


     private void delete(String localname){
        String [] name = localname.split("/");
        String filename = name[name.length-1];
        FileInfo.Builder deletebuilder = FileInfo.newBuilder();
        deletebuilder.setFilename(filename);
        int oldversion = getVersion(filename);
        int newversion = oldversion+1;
        deletebuilder.setVersion(newversion);
        FileInfo fileInfo = deletebuilder.build();
        FileInfo readInfo = metadataStub.readFile(fileInfo);
        if(!exist(readInfo)){
            System.out.println("NOT FOUND");
            return;
        }
        WriteResult result = metadataStub.deleteFile(fileInfo);
        while(result.getResult()!=WriteResult.Result.OK){
            oldversion = result.getCurrentVersion();
            newversion = oldversion+1;
            deletebuilder.setVersion(newversion);
            fileInfo = deletebuilder.build();
            result = metadataStub.deleteFile(fileInfo);
        }
        System.out.println("OK");
     }

     private void download(String path, String filename){
        FileInfo readInfo = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo fileInfo = metadataStub.readFile(readInfo);
        if(!exist(fileInfo)){
            System.out.println("NOT FOUND");
            return;
        }
        Map<String, byte[]> hashTobyte = new HashMap<>();
        List<String> hashlist = new ArrayList<>();
        for(int i=0;i<fileInfo.getBlocklistCount();i++){
            String hash = fileInfo.getBlocklist(i);
            hashlist.add(hash);
            hashTobyte.put(hash,null);
        }
        File filepath = new File(path);
        File [] filelist = filepath.listFiles();
        if(filelist!=null) {
            for (int i = 0; i < filelist.length; i++) {
                if (filelist[i].isFile()) {
                    logger.info("index of file " + String.valueOf(i));
                    ByteArrayOutputStream bos = new ByteArrayOutputStream((int) filelist[i].length());
                    BufferedInputStream in = null;
                    try {
                        in = new BufferedInputStream(new FileInputStream(filelist[i]));
                        int blk_size = 4096;
                        byte[] blockdata = new byte[blk_size];
                        int len = 0;
                        long remained = filelist[i].length();
                        while (remained>=blk_size&&-1 != (len = in.read(blockdata, 0, blk_size))) {
                            bos.write(blockdata, 0, len);
                            Block subfile = byteToBlock(blockdata);
                            String hash = subfile.getHash();
                            logger.info("existing hash is " +hash);
                            if (hashTobyte.containsKey(hash) && hashTobyte.get(hash) == null) {
                                hashTobyte.put(hash, blockdata);
                            }
                            remained -= blk_size;
                        }
                        byte [] lastblock = new byte[(int)remained];
                        len = in.read(lastblock,0,(int)remained);
                        bos.write(lastblock,0,len);
                        Block subfile = byteToBlock(lastblock);
                        String hash = subfile.getHash();
                        logger.info("existing hash is " +hash);
                        if(hashTobyte.containsKey(hash) && hashTobyte.get(hash)==null){
                            logger.info("adding hash is " +hash);
                            hashTobyte.put(hash,lastblock);
                        }
                        bos.close();
                        in.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        for(int i=0;i<hashlist.size();i++){
            String hash = hashlist.get(i);
            if(hashTobyte.get(hash)==null){
                logger.info("Read from block");
                Block downloadblock = Block.newBuilder().setHash(hash).build();
                Block block = blockStub.getBlock(downloadblock);
                byte [] blockdata = block.getData().toByteArray();
                hashTobyte.put(hash,blockdata);
            }
        }
         FileOutputStream fos = null;
         File file;
         try{
             file = new File(path + "/"+filename);
             fos = new FileOutputStream(file);
             for(int i=0;i<hashlist.size();i++){
                 String hash = hashlist.get(i);
                 byte [] data = hashTobyte.get(hash);
                 fos.write(data);
             }
             fos.flush();
         }catch (IOException e){
             e.printStackTrace();
         }finally {
             if (fos != null) {
                 try {
                     fos.close();
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             }
         }
         System.out.println("OK");
     }
     private int getVersion(String localname){
         String [] name = localname.split("/");
         String filename = name[name.length-1];
         FileInfo readInfo = FileInfo.newBuilder().setFilename(filename).build();
         FileInfo fileInfo = metadataStub.readFile(readInfo);
         int version = fileInfo.getVersion();
         int leaderVersion = version;
         String versionstr = String.valueOf(version);
         if(config.getNumMetadataServers()>1){
             for(int i=0;i<followers.length;i++){
                 fileInfo = followers[i].readFile(readInfo);
                 version = fileInfo.getVersion();
                 versionstr = versionstr+" "+String.valueOf(version);
             }
         }
         System.out.println(versionstr);
         return leaderVersion;
     }
     private void crash(int serverId){
        Empty empty = Empty.newBuilder().build();
        followers[serverId].crash(empty);
     }
     private void recover(int serverId){
        Empty empty = Empty.newBuilder().build();
        followers[serverId].restore(empty);
     }

	private void go() {
		 metadataStub.ping(Empty.newBuilder().build());
         logger.info("Successfully pinged the Metadata server");

         String filename="pom.xml";
         upload(filename);
         int version = getVersion(filename);
         logger.info("existing file version is" +String.valueOf(version));
         FileInfo readInfo = FileInfo.newBuilder().setFilename(filename).build();
         FileInfo fileInfo = metadataStub.readFile(readInfo);
         for(int i=0;i<fileInfo.getBlocklistCount();i++){
             String hash = fileInfo.getBlocklist(i);
             logger.info("block hash is "+hash);
         }
//         delete(filename);
//         fileInfo = metadataStub.readFile(readInfo);
//         version = getVersion(filename);
//        logger.info("delete file version is" +String.valueOf(version));
//        for(int i=0;i<fileInfo.getBlocklistCount();i++){
//            String hash = fileInfo.getBlocklist(i);
//            logger.info("delete block hash is "+hash);
//        }
        String path = "/Users/muyunliu/Documents/cse291-pj2/p2-p2-candys-master/java/src/";
        download(path,filename);
         blockStub.ping(Empty.newBuilder().build());
         logger.info("Successfully pinged the Blockstore server");

         Block b1 = stringToBlock("block_01");
         Block b2 = stringToBlock("block_02");
         //SimpleAnswer anwser = blockStub.hasBlock(b1);
         ensure(blockStub.hasBlock(b1).getAnswer() == false);
         ensure(blockStub.hasBlock(b2).getAnswer() == false);

         blockStub.storeBlock(b1);
         ensure(blockStub.hasBlock(b1).getAnswer() == true);

         blockStub.storeBlock(b2);
         ensure(blockStub.hasBlock(b2).getAnswer() == true);
         // TODO: Implement your client here

         Block b1prime = blockStub.getBlock(b1);
         ensure(b1.getHash().equals(b1prime.getHash()));
         ensure(b1.getData().equals(b1.getData()));
         String s0 = "s";

         logger.info("We passed all the tests");
	}

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {

        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("command").type(String.class).help("todo");
        parser.addArgument("name").type(String.class).help("filename or crashserver");
        if(args.length==4) {
            parser.addArgument("path").type(String.class).help("download path");
        }
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
        String filename = c_args.getString("name");
        String command = c_args.getString("command").toLowerCase();
        String localpath = null;
        if(args.length==4){
            localpath = c_args.getString("path");
        }
        logger.info("command "+ command);
        logger.info("localpath "+ localpath);
        logger.info("filename is "+ filename);

        Client client = new Client(config);
        
        try {
        	//client.go();
            if(command.equals("getversion")){
                client.getVersion(filename);
            }
            else if(command.equals("upload")){
                client.upload(filename);
            }
            else if(command.equals("delete")){
                client.delete(filename);
            }
            else if(command.equals("download")){
                client.download(localpath,filename);
            }
            else if(command.equals("crash")){
                int follow = Integer.parseInt(filename) - 2;
                client.crash(follow);
            }
            else if(command.equals("recover")){
                int follow = Integer.parseInt(filename) - 2;
                client.recover(follow);
            }
        } finally {
            client.shutdown();
        }
    }

    static class HashUtils{
        //public HashUtils(){};

        private static String sha256(String s){
            MessageDigest digest = null;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                System.exit(2);
            }
            byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
            String encoded = Base64.getEncoder().encodeToString(hash);

            return encoded;
        }
        private static String sha256byte(byte[] s){
            MessageDigest digest = null;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                System.exit(2);
            }
            byte[] hash = digest.digest(s);
            String encoded = Base64.getEncoder().encodeToString(hash);
            return encoded;

        }
    }

}
