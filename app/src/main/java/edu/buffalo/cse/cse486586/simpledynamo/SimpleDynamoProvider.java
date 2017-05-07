package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.Buffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    //********************************** begin
//    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final String TAG = "SimpleDynamoProvider";
    static final String[] REMOTE_PORTS = new String[]{"11108","11112","11116","11120","11124"};
    static final int SERVER_PORT = 10000;
    static final int TIMEOUT = 100;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String DB_NAME = "my";
    private static final int DB_VERSION = 1;
    private static final String REGEX = ",,,";

    private static String selfPort;
    private static String selfHash;
//    private static String predecessor;
//    private static String successor;
    private static ArrayList<String> allNodes = new ArrayList<String>();
    HashMap<String, Socket> allSockets = new HashMap<String, Socket>();

    // 0:初始化阶段， 1：normal 阶段。遇到新的 join 需要特殊处理
    private static HashMap<String, Boolean> allServerStatus = new HashMap<String, Boolean>();

    private SQLiteDatabase db;
    private MySQLiteOpenHelper mySQLiteOpenHelper;


    private static class MySQLiteOpenHelper extends SQLiteOpenHelper {
        public MySQLiteOpenHelper(Context context, String name,
                                  SQLiteDatabase.CursorFactory factory, int version){
            super(context, name, factory, version);
        }

        private static final String DB_CREATE = "create table " + DB_NAME + " (key primary key, value);" ;

        @Override
        public void onCreate(SQLiteDatabase db){
            db.execSQL(DB_CREATE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion){
            db.execSQL("drop table if exists " + DB_NAME);
            onCreate(db);
        }
    }
    ///******************************** end



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

        if (selection.equals("@")){
            return localDelete("*All*");
        } else if (selection.equals("*")) {
            for (int i = 0; i < allNodes.size(); i++) {
                if(allNodes.get(i).equals(selfPort)){
                    localDelete("*ALL*");
                    continue;
                }

                if(allSockets.get(allNodes.get(i)) != null){
                    try{
                        String strSend = "DELETE" + REGEX + "*ALL*";
                        Writer writer = new OutputStreamWriter(allSockets.get(allNodes.get(i)).getOutputStream());
                        writer.write(strSend+"\n");
                        writer.flush();
                    }catch (Exception e){
                        //错误发生，善后
                        try{
                            allSockets.get(allNodes.get(i)).close();
                        }catch (Exception ee){
                            logPrint(ee.getMessage());
                        }
                        allSockets.put(allNodes.get(i), null);
                        logPrint("删除命令发送过程中发生可接收错误4");
                    }
                }
            }
        } else {
            String rNode = findResponsibleNode(selection);
            String rNode1 = findSuccessor(rNode);
            String rNode2 = findSuccessor(rNode1);

            String[] responsibleNodes = new String[]{rNode, rNode1, rNode2};
            for (String rn: responsibleNodes){
                //如果是自己就直接删，减小负担
                if(rn.equals(selfPort)){
                    localDelete(selection);
                    continue;
                }

                if(allSockets.get(rn) != null){
                    try{
                        String strSend = "DELETE" + REGEX + selection;
                        Writer writer = new OutputStreamWriter(allSockets.get(rn).getOutputStream());
                        writer.write(strSend+"\n");
                        writer.flush();
                    }catch (Exception e){
                        //错误发生，善后
                        try{
                            allSockets.get(rn).close();
                        }catch (Exception ee){
                            logPrint(ee.getMessage());
                        }
                        allSockets.put(rn, null);
                        logPrint("删除命令发送中该发生可以发生的错误！74");
                    }
                }
            }
        }
        return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        logPrint("[insert]Inserting1....I got the values: "+ values.getAsString(KEY_FIELD) + " "+ values.getAsString(VALUE_FIELD));
        String responsibleNode = findResponsibleNode(values.getAsString(KEY_FIELD));
        String strSend = values.getAsString(KEY_FIELD) + REGEX + values.getAsString(VALUE_FIELD);

        logPrint("[insert] now the responsibleNode is "+responsibleNode);
        logPrint("[insert]Inserting....Found the responsibleNode: "+responsibleNode);

        if(allSockets.get(responsibleNode) != null){
            //负责的节点记录中没有崩，但是可能在写东西的时候崩溃
            try{
                Writer writer = new OutputStreamWriter(allSockets.get(responsibleNode).getOutputStream());
                String strSend1 = "INSERT" + REGEX + "HEAD" +REGEX+ strSend;
                writer.write(strSend1+"\n");
                writer.flush();
            } catch (Exception e){
                //检测到传输异常
                try {
                    allSockets.get(responsibleNode).close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                allSockets.put(responsibleNode, null);

                // 改用发送下一个负责节点， MID
                try{
                    Writer writer = new OutputStreamWriter(allSockets.get(findSuccessor(responsibleNode)).getOutputStream());
                    String strSend2 = "INSERT" + REGEX + "MID" + REGEX + strSend;
                    writer.write(strSend2+"\n");
                    writer.flush();
                }catch (Exception e1){
                    logPrint("[insert] 不能出现双重崩溃！"+ e1.getMessage());
                }
                logPrint(e.getMessage());
            }
        }else{
            //负责的节点在已知的情况已经崩溃了,发送给 MID
            try{
                Writer writer = new OutputStreamWriter(allSockets.get(findSuccessor(responsibleNode)).getOutputStream());
                String strSend3 = "INSERT" + REGEX + "MID" + REGEX + strSend;
                writer.write(strSend3+"\n");
                writer.flush();
            }catch (Exception e1){
                logPrint("[insert] 不能出现双重崩溃！"+ e1.getMessage());
            }
        }
        return null;
	}

	@Override
	public boolean onCreate() {
        Context context = getContext();
        mySQLiteOpenHelper = new MySQLiteOpenHelper(context, DB_NAME, null, DB_VERSION);

        //Calculate the port number that this AVD listens on.
        TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        selfPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try{
            selfHash = genHash( String.valueOf(Integer.valueOf(selfPort)/2));
        }catch (NoSuchAlgorithmException e){
            logPrint(e.getMessage());
        }

        //构建 hash 环, 并创建空的 allSockets
        for(String item: REMOTE_PORTS){
            allNodes.add(item);
            allSockets.put(item, null);
            allServerStatus.put(item, false);
        }
        allNodes = sortByHashValue(allNodes);

        logPrint(selfPort);
        logPrint(selfHash);

        // new a ServerTask()
        try{
            logPrint("Begin to get into serversocket");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
//            new SendJoinTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new SendJoinTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, serverSocket);

        }catch (IOException e){
            Log.e(TAG, "Can't create a ServerSocket");
        }

        //endtest

        //todo 需要处理新加进来的 node 的数据库问题。初步想法：可以在 onjoin 处处理。



		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        if(selection.equals("@")) {
            return localQuery(null);
        }else if(selection.equals("*")){

            //写一个函数返回的是一个list，里面是当前机器应该负主要责任的kv 对。参数可以设定找到主要责任，次要责任的。
            // 万一它前面的几点崩溃了，数据存在这里
            MatrixCursor myCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            for (int i = 0; i < allNodes.size(); i++) {
                String currentNode = allNodes.get(i);

                if(allSockets.get(currentNode) != null){
                    try{
                        String strSend = "QUERY"+REGEX+"*ONLYSELF*";
                        Writer writer = new OutputStreamWriter(allSockets.get(currentNode).getOutputStream());
                        writer.write(strSend+"\n");
                        writer.flush();

                        //接收数据
                        BufferedReader br = new BufferedReader(new InputStreamReader(allSockets.get(currentNode).getInputStream()));
                        String[] queryResponses = br.readLine().split(REGEX);
                        for (int j = 1; j < queryResponses.length; j+=2) {
                            myCursor.addRow(new String[]{queryResponses[j], queryResponses[j+1]});
                        }
                    }catch (Exception e){
                        //处理错误
                        logPrint("1082"+e.getMessage());
                        try{
                            allSockets.get(currentNode).close();
                        }catch (Exception ee){
                            logPrint(ee.getMessage());
                        }
                        allSockets.put(currentNode, null);

                        //从他的后一个读取
                        try{
                            String strSend = "QUERY"+REGEX+"*ONLYPRED*";
                            Writer writer = new OutputStreamWriter(allSockets.get(findSuccessor(currentNode)).getOutputStream());
                            writer.write(strSend+"\n");
                            writer.flush();

                            //接收数据
                            BufferedReader br = new BufferedReader(new InputStreamReader(allSockets.get(findSuccessor(currentNode)).getInputStream()));
                            String[] queryResponses = br.readLine().split(REGEX);
                            for (int j = 1; j < queryResponses.length; j+=2) {
                                myCursor.addRow(new String[]{queryResponses[j], queryResponses[j+1]});
                            }
                        }catch (Exception ee){
                            logPrint("这里不能崩溃64");
                        }
                    }
                }else{
                    //从他的后一个读取
                    try{
                        String strSend = "QUERY"+REGEX+"*ONLYPRED*";
                        Writer writer = new OutputStreamWriter(allSockets.get(findSuccessor(currentNode)).getOutputStream());
                        writer.write(strSend+"\n");
                        writer.flush();

                        //接收数据
                        BufferedReader br = new BufferedReader(new InputStreamReader(allSockets.get(findSuccessor(currentNode)).getInputStream()));
                        String[] queryResponses = br.readLine().split(REGEX);
                        for (int j = 1; j < queryResponses.length; j+=2) {
                            myCursor.addRow(new String[]{queryResponses[j], queryResponses[j+1]});
                        }
                    }catch (Exception ee){
                        logPrint("这里不能崩溃64");
                    }
                }
            }
            return myCursor;
        }else {
            MatrixCursor myCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            String responsibleNode = findResponsibleNode(selection);

            logPrint("[query] selection is " + selection + " and responsibleNode is " + responsibleNode);

            String tailnode = findSuccessor(findSuccessor(responsibleNode));
            if (allSockets.get(responsibleNode) != null) {
                String strSend = "QUERY" + REGEX + selection;
                try {
                    Writer writer = new OutputStreamWriter(allSockets.get(tailnode).getOutputStream());
                    writer.write(strSend + "\n");
                    writer.flush();
                    logPrint("[query] Finish sending query command: " + strSend + " , and wait feedback.....");

                    BufferedReader br = new BufferedReader(new InputStreamReader(allSockets.get(tailnode).getInputStream()));
                    String tmp = br.readLine();
                    logPrint("[query] I got feedback: " + tmp);

                    String[] queryResponses = tmp.split(REGEX);
                    for (int j = 1; j < queryResponses.length; j += 2) {
                        myCursor.addRow(new String[]{queryResponses[j], queryResponses[j + 1]});
                    }
                    return myCursor;

                } catch (Exception e) {
                    logPrint(e.getMessage());
                    //中途遇到了崩溃
                    try {
                        allSockets.get(tailnode).close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    allSockets.put(tailnode, null);

                    //转头开始向上一个查询
                    try {
                        Writer writer = new OutputStreamWriter(allSockets.get(findPredecessor(tailnode)).getOutputStream());
                        writer.write(strSend + "\n");
                        writer.flush();
                        logPrint("[query] Finish sending query command: " + strSend + " , and wait feedback.....");

                        BufferedReader br = new BufferedReader(new InputStreamReader(allSockets.get(findPredecessor(tailnode)).getInputStream()));
                        String tmp = br.readLine();
                        br.close();
                        logPrint("[query] I got feedback: " + tmp);

                        String[] queryResponses = tmp.split(REGEX);
                        for (int j = 1; j < queryResponses.length; j += 2) {
                            myCursor.addRow(new String[]{queryResponses[j], queryResponses[j + 1]});
                        }
                        return myCursor;
                    } catch (Exception ee) {
                        logPrint("不能在这里发生崩溃" + ee.getMessage());
                    }
                    logPrint(e.getMessage());
                }
            } else {
                String strSend = "QUERY" + REGEX + selection;
                try {
                    Writer writer = new OutputStreamWriter(allSockets.get(findPredecessor(tailnode)).getOutputStream());
                    writer.write(strSend + "\n");
                    writer.flush();
                    logPrint("[query] Finish sending query command: " + strSend + " , and wait feedback.....");

                    BufferedReader br = new BufferedReader(new InputStreamReader(allSockets.get(findPredecessor(tailnode)).getInputStream()));
                    String tmp = br.readLine();
                    br.close();
                    logPrint("[query] I got feedback: " + tmp);

                    String[] queryResponses = tmp.split(REGEX);
                    for (int j = 1; j < queryResponses.length; j += 2) {
                        myCursor.addRow(new String[]{queryResponses[j], queryResponses[j + 1]});
                    }
                    return myCursor;
                } catch (Exception ee) {
                    logPrint("不能在这里发生崩溃" + ee.getMessage());
                }
            }
        }
        return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private void localInsert(ContentValues values){
        db = mySQLiteOpenHelper.getWritableDatabase();
        try{
            db.insertWithOnConflict(DB_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
        } catch (Exception e){
            Log.e(TAG, "Error in qb local_inserting");
        }
        Log.v("localInsert", values.toString());
    }

    public Cursor localQuery(String selection) {
        ///**************** begin

        db = mySQLiteOpenHelper.getWritableDatabase();
        String sql;
        if(selection == null){
            sql = "select * from " + DB_NAME;
        }else{
            sql = "select * from " + DB_NAME + " where " + "key=\"" + selection +"\"";
        }
        Cursor cursor = db.rawQuery(sql, null);

        ///**************** end

        if(selection != null){
            logPrint("query: "+selection);
        }else{
            logPrint("[localQuery] query everything");
        }
        return cursor;
    }

    private int localDelete(String selection){
        if (selection.equals("*ALL*")) {
            db = mySQLiteOpenHelper.getWritableDatabase();
            return db.delete(DB_NAME, null, null);  //delete all

        }else{
            db = mySQLiteOpenHelper.getWritableDatabase();
            return db.delete(DB_NAME, "key='"+selection + "'", null);
        }


    }

    public ArrayList<String> localQueryOnlySelf(){
        ArrayList<String> retList = new ArrayList<String>();

        Cursor retCursor = localQuery(null);

        //过滤结果放入 list 中
        retCursor.moveToFirst();
        while (!retCursor.isAfterLast()){
            if(findResponsibleNode(retCursor.getString(0)).equals(selfPort)){
                retList.add(retCursor.getString(0));
                retList.add(retCursor.getString(1));
            }
            retCursor.moveToNext();
        }
        return retList;
    }
    public ArrayList<String> localQueryOnlyPredecessor(){
        ArrayList<String> retList = new ArrayList<String>();

        Cursor retCursor = localQuery(null);

        //过滤结果放入 list 中
        retCursor.moveToFirst();
        while (!retCursor.isAfterLast()){
            if(findResponsibleNode(retCursor.getString(0)).equals(findPredecessor(selfPort))){
                retList.add(retCursor.getString(0));
                retList.add(retCursor.getString(1));
            }
            retCursor.moveToNext();
        }
        return retList;
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void logPrint(String mm){
        if(mm != null){
            Log.e(TAG, mm);
        }
    }

    private ArrayList<String> sortByHashValue(ArrayList<String> inputList){
        // Sorting
        Collections.sort(inputList, new Comparator<String>() {
            @Override
            public int compare(String lhsOriginal, String rhsOriginal)
            {
                String lhs = String.valueOf(Integer.valueOf(lhsOriginal)/2);
                String rhs = String.valueOf(Integer.valueOf(rhsOriginal)/2);
                int flag;
                try{
                    flag = genHash(lhs).compareTo(genHash(rhs));
                    return flag;
                }catch (Exception e){
                    logPrint(e.getMessage());
                }
                return -1;
            }
        });
        return inputList;
    }

    private String findResponsibleNode(String keyStr){

        String keyStrHash;
        try {
            keyStrHash = genHash(keyStr);
        }catch (NoSuchAlgorithmException e){
            logPrint(e.getMessage());
            return null;
        }

        for (int i = 0; i < allNodes.size(); i++) {
            String currentNodeHash;
            try{
                currentNodeHash = genHash( String.valueOf(Integer.valueOf(allNodes.get(i))/2));
            }catch (NoSuchAlgorithmException e){
                logPrint(e.getMessage());
                return null;
            }
            if(keyStrHash.compareTo(currentNodeHash) <= 0){
                return allNodes.get(i);
            }
        }
        return allNodes.get(0);
    }

    private String findSuccessor(String port){
        // 11124 - 11112 - 11108 - 11116 - 11120
        if(port.equals("11124")){
            return "11112";
        }else if(port.equals("11112")){
            return "11108";
        }else if(port.equals("11108")){
            return "11116";
        }else if(port.equals("11116")){
            return "11120";
        }else if(port.equals("11120")){
            return "11124";
        }else{
            return null;
        }
    }
    private String findPredecessor(String port){
        if(port.equals("11124")){
            return "11120";
        }else if(port.equals("11120")){
            return "11116";
        }else if(port.equals("11116")){
            return "11108";
        }else if(port.equals("11108")){
            return "11112";
        }else if(port.equals("11112")){
            return "11124";
        }else{
            return null;
        }
    }

    public class SendJoinTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            for (int i = 0; i < REMOTE_PORTS.length; i++) {
                Socket socket0;
                while (true){
                    //和所有的其他 avd 建立 socket， 由于启动是按顺序的
//                    logPrint("Try the remote-port : "+ REMOTE_PORTS[i]);
                    try{
                        socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.valueOf(REMOTE_PORTS[i]));

                        Scanner scanner = new Scanner(socket0.getInputStream());
                        long startTime = System.currentTimeMillis();
                        boolean isDone = false;
                        while ((System.currentTimeMillis() - startTime) < TIMEOUT){
                            if (scanner.hasNext()){
                                String sb = scanner.nextLine();
//                                logPrint("Got from "+REMOTE_PORTS[i]+ " " +sb);
                                allSockets.put(REMOTE_PORTS[i], socket0);
                                isDone = true;
                                break;
                            }

                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e3) {
                                e3.printStackTrace();
                            }
                        }

                        //跳出二重循环
                        if(!isDone){
                            socket0.close();
                        }else{
                            break;
                        }

                    }catch (Exception e){
                        logPrint(e.getMessage());
                    }
                }
                //发送一个 join，表明 selfport

                try {
                    OutputStreamWriter writer = new OutputStreamWriter(socket0.getOutputStream());
                    String strToSend = "JOIN" + REGEX + selfPort;
                    writer.write(strToSend + "\n");
                    writer.flush();

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            //设定当前已经处理 normal 阶段，所有 avd 都已经连接


            //test 结束之前测试socket 是否正常传输
//            try {
//                OutputStreamWriter writer = new OutputStreamWriter(allSockets.get("11116").getOutputStream());
//                String strToSend = "SHOW" + REGEX + "Seeeeeeeeeeeeeeeeeeeee_from_"+selfPort;
//                writer.write(strToSend + "\n");
//                writer.flush();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            //endtest

            return null;
        }
    }

    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
//            ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
            while (true) {
                try {
                    Socket socket_sv = serverSocket.accept();
                    logPrint("I accpted a new socket");
                    cachedThreadPool.execute(new AsServer(socket_sv));
//                    singleThreadExecutor.execute(new AsServer(socket_sv));
                } catch (Exception e) {
                    Log.e(TAG, "error bufferedReader" + e.getMessage());
                }
            }
//            return null;
        }

        class AsServer implements Runnable {
            private Socket socket_accepted;
            private String socket_port;

            AsServer(Socket sckt) {
                this.socket_accepted = sckt;
            }

            public void run() {
                try{
                    OutputStreamWriter writer = new OutputStreamWriter(socket_accepted.getOutputStream());
                    writer.write("ACK\n");
                    writer.flush();
                }catch (Exception e){
                    logPrint(e.getMessage());
                }

                while (true){
                    try{
                        String msg;
                        BufferedReader br = new BufferedReader(new InputStreamReader(socket_accepted.getInputStream()));
                        msg = br.readLine();
                        dealwithCommand(socket_accepted, msg);
                    }catch (Exception e){
                        logPrint("断开: "+socket_port);
                        try{
                            socket_accepted.close();
                            allSockets.get(socket_port).close();
                        }catch (Exception ee){
                            logPrint(ee.getMessage());
                        }
                        allSockets.put(socket_port, null); //设置这个向外的连接口不能用
                        break; //跳出当前循环，退出子进程
                    }
                    logPrint("00");
                }
                logPrint("Asserver 子线程走到了尽头");

            }
            private void dealwithCommand(Socket socket, String cmd){
                String[] tokens = cmd.split(REGEX,2);

                if(tokens[0].equals("JOIN")){
                    onJOIN(tokens[1]);
                }else if(tokens[0].equals("INSERT")){
                    onINSERT(tokens[1]);
                }else if(tokens[0].equals("QUERY")) {
                    onQUERY(socket, tokens[1]);
                }else if(tokens[0].equals("DELETE")){
//                    onDELETE(tokens[1]);
                }
                else if(tokens[0].equals("SHOW")){
                    logPrint("From "+socket_port + " : "+ tokens[1]);
                }

                else{
                    logPrint("[dealwithCommand] This commander is not supported!");
                }
            }
            private void onJOIN(String content){
                logPrint("GOT JOIN MESSAGE from : "+content);
                socket_port = content;

                if(allServerStatus.get(socket_port)){
                    //创建新的 socket 到对方并保存
                    Socket socket0;
                    try{
                        socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.valueOf(socket_port));
                        BufferedReader br = new BufferedReader(new InputStreamReader(socket0.getInputStream()));
                        br.readLine();

                        allSockets.put(socket_port, socket0);

                        Writer writer = new OutputStreamWriter(socket0.getOutputStream());
                        writer.write("JOIN"+REGEX+selfPort+"\n");
                        writer.flush();

                    }catch (Exception e){
                        logPrint(e.getMessage());
                    }

                    //todo, 这里可以调用 allsockets 的对外 socket 来发送他可能需要的数据。

                }
                allServerStatus.put(socket_port, true);
            }

            private void onINSERT(String content){
                logPrint("[Server:onINSERT] try to insert content: " + content);
                String[] tokens = content.split(REGEX);

                //先存到自己的数据库里， 构造 contentValues 并插入。
                for (int i = 1; i < tokens.length; i += 2) {
                    ContentValues t = new ContentValues();
                    t.put(KEY_FIELD, tokens[i]);
                    t.put(VALUE_FIELD, tokens[i+1]);
                    localInsert(t);
                }

                //判断是否要发送给后面
                if(tokens[0].equals("HEAD")){

                    if(allSockets.get(findSuccessor(selfPort)) != null){
                        try{
                            String strSend = "INSERT" + REGEX + "MID";
                            for (int i = 1; i < tokens.length; i += 2) {
                                strSend = strSend + REGEX + tokens[i] + REGEX + tokens[i+1];
                            }

                            Writer writer = new OutputStreamWriter(allSockets.get(findSuccessor(selfPort)).getOutputStream());
                            writer.write(strSend+"\n");
                            writer.flush();
                        }catch (Exception e){
                            //检测到传输异常
                            try {
                                allSockets.get(findSuccessor(selfPort)).close();
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                            allSockets.put(findSuccessor(selfPort), null);
                            //继续传输
                            try{
                                String strSend = "INSERT" + REGEX + "TAIL";
                                for (int i = 1; i < tokens.length; i += 2) {
                                    strSend = strSend + REGEX + tokens[i] + REGEX + tokens[i+1];
                                }

                                Writer writer = new OutputStreamWriter(allSockets.get(findSuccessor(findSuccessor(selfPort))).getOutputStream());
                                writer.write(strSend+"\n");
                                writer.flush();
                            }catch (Exception e1){
                                logPrint("不能有崩溃"+e1.getMessage());
                            }
                            logPrint(""+e.getMessage());
                        }
                    }else{
                        try{
                            String strSend = "INSERT" + REGEX + "TAIL";
                            for (int i = 1; i < tokens.length; i += 2) {
                                strSend = strSend + REGEX + tokens[i] + REGEX + tokens[i+1];
                            }

                            Writer writer = new OutputStreamWriter(allSockets.get(findSuccessor(findSuccessor(selfPort))).getOutputStream());
                            writer.write(strSend+"\n");
                            writer.flush();
                        }catch (Exception e){
                            logPrint("不能有崩溃"+e.getMessage());
                        }
                    }

                }else if(tokens[0].equals("MID")){
                    if(allSockets.get(findSuccessor(selfPort)) != null){
                        try{
                            String strSend = "INSERT" + REGEX + "TAIL";
                            for (int i = 1; i < tokens.length; i += 2) {
                                strSend = strSend + REGEX + tokens[i] + REGEX + tokens[i+1];
                            }

                            Writer writer = new OutputStreamWriter(allSockets.get(findSuccessor(selfPort)).getOutputStream());
                            logPrint("4, try to send: "+strSend);
                            writer.write(strSend+"\n");
                            writer.flush();
                        }catch (Exception e){
                            //检测到传输异常
                            try {
                                allSockets.get(findSuccessor(selfPort)).close();
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                            allSockets.put(findSuccessor(selfPort), null);
                            logPrint(""+e.getMessage());
                        }

                    }else {
                        ; //do nothing
                    }

                }
            }

            private void onQUERY(Socket socket, String content){
                //接收 <哪里发来的 query>，<query 什么>，→ 然后socket发回去。（找不到的话，发 null）
                logPrint("Now we are in onQUERY");
                String[] tokens = content.split(REGEX);
                String queryKey = tokens[0];
                Cursor retCursor;
                if (queryKey.equals("*ALL*")){
                    retCursor = localQuery(null);
                }else if(queryKey.equals("*ONLYSELF*") || queryKey.equals("ONLYPRED")){
                    ArrayList<String> listToSend;
                    if(queryKey.equals("*ONLYSELF*")){
                       listToSend = localQueryOnlySelf();
                    }else{
                        listToSend = localQueryOnlyPredecessor();
                    }
                    String strSend = "QUERYRESPONSE" + REGEX;
                    for (int i = 0; i < listToSend.size(); i = i+2) {
                        strSend += listToSend.get(i) + REGEX + listToSend.get(i+1)+REGEX;
                    }

                    try{
                        Writer writer = new OutputStreamWriter(socket.getOutputStream());
                        writer.write(strSend+"\n");
                        logPrint("[onQUERY_ONLYself] sending msg: "+strSend);
                        writer.flush();
                    }catch (Exception e){
                        logPrint(e.getMessage());
                    }
                    return;
                } else{
                    retCursor = localQuery(queryKey);
                }

                //构造发送字符串
                String strSend = "";
                retCursor.moveToFirst();
                while (!retCursor.isAfterLast()){
                    strSend += retCursor.getString(0) + REGEX + retCursor.getString(1) + REGEX;
                    retCursor.moveToNext();
                }
                //整理 retCursor 并发送
                try{
                    OutputStreamWriter myWrite = new OutputStreamWriter(socket.getOutputStream());
                    strSend = "QUERYRESPONSE" + REGEX + strSend;
                    logPrint("[onQUERY] sending msg: "+strSend);
                    myWrite.write(strSend+"\n");
                    myWrite.flush();
                }catch (Exception e){
                    logPrint(e.getMessage());
                }

            }

            private void onDELETE(String content){
                localDelete(content);
            }

        }
    }


}
