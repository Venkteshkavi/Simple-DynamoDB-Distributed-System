package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.apache.http.impl.io.ContentLengthInputStream;

public class SimpleDynamoProvider extends ContentProvider {

	//Global Variables
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	String globalPort;
	static final int SERVER_PORT = 10000;
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
    int insertCount = 0;
    String queryToReturn = null;
    int QueryRcxCount = 0;
    int globalRcxEnd=0;
	String key_hash;
	String portToSend ;
	int partionFlag =0 ;
    int startIdx = 0;
    int currentPortComp = 0;
    int prevPortComp = 0;
    int finalPortListSize = 0;
    String replPort1;
    String replPort2;
    String serverDownCheck;
    String serverDownConfirmation;
    String predecessor_1;
    String predecessor_2;
    String successor_1;
    String successor_2;
    String predecessor_1ID;
    String successor_1ID;
    int InsertCount = 0;


    ArrayList<String> finalPortList = new ArrayList<String>();
    HashMap<String,String> localMap = new HashMap<String, String>();
    HashMap<String,String> globalFilesMap = new HashMap<String, String>();
    HashMap<String,String> replicatedMap = new HashMap<String, String>();
    ArrayList<String> localArray = new ArrayList<String>();
    ArrayList<String> globalFilesArray = new ArrayList<String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
	    if(selection.equals("@")){
	        Log.d(TAG,"DELETION " + selection);
	        localMap.clear();
	        Log.d(TAG,"LOCAL MAP " + localMap);
        }
        else if(selection.equals("*")){
            globalFilesMap.clear();
            Log.d(TAG,"Global Map " + globalFilesMap);
        }

        else{
            replicatedMap.clear();
            localMap.clear();
            globalFilesMap.clear();
            Log.d(TAG,"ALL MAPS GETTING CLEARED");
        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	synchronized public Uri insert(Uri uri, ContentValues values) {
            String keyValToSend = "";
            String content_values = values.toString();
            String[] cv_arr = content_values.split(" ");
            String[] key_arr = cv_arr[0].split("=");
            String[] val_arr = cv_arr[1].split("=");
            try {
                key_hash = genHash(val_arr[1]);
                String portToForward = partitionIdentifier(key_hash);
                keyValToSend = val_arr[1] + ":" + key_arr[1];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValToSend, portToForward);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Error in Hashing " + e);
            } catch (Exception e) {
                Log.d(TAG, "Error In Insertion Code " + e);
            }

            return null;
    }

	public String partitionIdentifier(String key_hash){
	    String portGen = "";
        if(globalPort.equals("5556")){
            startIdx = 1;
        }
        else if(globalPort.equals("5554")){
            startIdx = 2;
        }
        else if(globalPort.equals("5558")){
            startIdx = 3;
        }
        else if(globalPort.equals("5560")){
            startIdx = 4;
        }
        else{
            startIdx = 5;
        }
        partionFlag = 0;
        for (int i = startIdx - 1; i < startIdx + 4; i++) {
            String[] split_str = finalPortList.get(i).split(":");
            String[] split_str1 = finalPortList.get(i + 1).split(":");
            prevPortComp = key_hash.compareTo(split_str[0]);
            currentPortComp = key_hash.compareTo(split_str1[0]);

            // CONDITION CHECKING

            if (prevPortComp > 0 && currentPortComp <= 0) {
                portToSend = split_str1[1];
                if(portToSend.equals("5562")){
                    replPort1 = "5556";
                    replPort2 = "5554";
                }
                else if(portToSend.equals("5556")){
                    replPort1 = "5554";
                    replPort2 = "5558";
                }
                else if(portToSend.equals("5554")){
                    replPort1 = "5558";
                    replPort2 = "5560";
                }

                else if(portToSend.equals("5558")){
                    replPort1 = "5560";
                    replPort2 = "5562";
                }

                else{
                    replPort1 = "5562";
                    replPort2 = "5556";
                }
                portGen = portToSend + ":" + replPort1 + ":" + replPort2;
                partionFlag = 1;
                break;
            }
            else if (currentPortComp < 0 && prevPortComp < 0)
                continue;

            else if (currentPortComp > 0 && prevPortComp < 0 && currentPortComp > prevPortComp)
                continue;

            else if (currentPortComp == prevPortComp && currentPortComp < 0 && prevPortComp < 0) {
                continue;
            }
        }
        if (partionFlag == 0) {
            portToSend = "5562";
            portGen = portToSend + ":" + "5554" + ":" + "5556";
//                Log.d(TAG,"Port To Send is --" + portToSend + "KEY-VALUE IS --" + keyValToSend);
        }
            return portGen;
    }

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		globalPort = Integer.toString(Integer.parseInt(myPort) / 2);
		Log.d(TAG, globalPort);
		SimpleDynamoProvider obj = new SimpleDynamoProvider();


		//*************** PARTITIONING ***********************

		try {
			ArrayList<String> portList = new ArrayList<String>() {
				{
					add(genHash("5562") + ":" + "5562");
					add(genHash("5556") + ":" + "5556");
					add(genHash("5554") + ":" + "5554");
					add(genHash("5558") + ":" + "5558");
					add(genHash("5560") + ":" + "5560");
					add(genHash("5562") + ":" + "5562");
					add(genHash("5556") + ":" + "5556");
					add(genHash("5554") + ":" + "5554");
					add(genHash("5558") + ":" + "5558");
					add(genHash("5560") + ":" + "5560");
				}
			};


			finalPortListSize = portList.size();
            finalPortList.addAll(portList);
            if(globalPort.equals("5554")){
                predecessor_1 = "5556";
                predecessor_2 = "5562";
                successor_1 = "5558";
                successor_2 = "5560";
                predecessor_1ID = REMOTE_PORT1;
                successor_1ID = REMOTE_PORT2;
            }
            else if(globalPort.equals("5556")){
                predecessor_1 = "5562";
                predecessor_2 = "5560";
                successor_1 = "5554";
                successor_2 = "5558";
                predecessor_1ID = REMOTE_PORT4;
                successor_1ID = REMOTE_PORT0;
            }

            else if(globalPort.equals("5558")){
                predecessor_1 = "5554";
                predecessor_2 = "5556";
                successor_1 = "5560";
                successor_2 = "5562";
                predecessor_1ID = REMOTE_PORT0;
                successor_1ID = REMOTE_PORT3;
            }

            else if(globalPort.equals("5560")){
                successor_1 = "5562";
                successor_2 = "5556";
                predecessor_1 = "5558";
                predecessor_2 = "5554";
                predecessor_1ID = REMOTE_PORT2;
                successor_1ID = REMOTE_PORT4;
            }

            else{
                predecessor_1 = "5560";
                predecessor_2 = "5558";
                successor_1 = "5556";
                successor_2 = "5554";
                predecessor_1ID = REMOTE_PORT3;
                successor_1ID = REMOTE_PORT1;
            }

            Log.d(TAG,"MY PORT PREDECESSORS 1 -- " +predecessor_1);
            Log.d(TAG,"MY PORT PREDECESSORS 2 -- " + predecessor_2);

		}catch(NoSuchAlgorithmException e){
			Log.e(TAG,"Error in Hashing " + e);
		}

		//**************** SERVER CREATION ***********************

		try{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.d("SERVER CREATED","@" + globalPort);
		}catch(Exception e){
			Log.e(TAG,"Error in Server Creation " + e);
		}


        // RECOVERY MECHANISM


            try {
                new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "FAILURE", myPort);
            } catch (Exception e) {
                Log.e(TAG, "ERROR IN CREATING REOCOVERY MECHANISM " + e);
            }


		return false;
	}

	@Override
	synchronized public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
            String[] columnName = new String[2];
            columnName[0] = "key";
            columnName[1] = "value";
            MatrixCursor mCursor = new MatrixCursor(columnName);

            if (selection.equals("@")) {
                Log.d(TAG, "Querying" + selection);
                Log.i(TAG, "LOCAL MAP TO TRAVERSE  " + localMap + "  --" + globalPort);
                for (HashMap.Entry<String, String> item : localMap.entrySet()) {
                    MatrixCursor.RowBuilder mRowCursor = mCursor.newRow();
                    String keyToInsert = item.getKey();
                    String valToInsert = item.getValue();
                    mRowCursor.add(columnName[0], keyToInsert);
                    mRowCursor.add(columnName[1], valToInsert);
                }
            } else if (selection.equals("*")) {

                try {
                    String request = "SENDALL";
                    globalFilesMap.putAll(localMap);
                    String[] clientPorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
                    for (String remortPort : clientPorts) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remortPort));
                            ArrayList<String> queryToSend = new ArrayList<String>();
                            queryToSend.add("SENDALL" + ":" + globalPort);
                            OutputStream outputStream = socket.getOutputStream();
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                            objectOutputStream.writeObject(queryToSend);
                            Log.d(TAG, "QUERY REQUEST SENT FOR COLLECTING FILES ");
                            outputStream.flush();
                            InputStream inputStream = socket.getInputStream();
                            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                            HashMap<String, String> queryFilesReceieved = (HashMap<String, String>) objectInputStream.readObject();
                            globalFilesMap.putAll(queryFilesReceieved);
                            for (HashMap.Entry<String, String> item : globalFilesMap.entrySet()) {
                                MatrixCursor.RowBuilder mRowCursor = mCursor.newRow();
                                String keyToInsert = item.getKey();
                                String valToInsert = item.getValue();
                                mRowCursor.add(columnName[0], keyToInsert);
                                mRowCursor.add(columnName[1], valToInsert);
                            }
                        } catch (Exception e) {

                        }
                        globalFilesMap.clear();

                    }
                } catch (Exception e) {
                    Log.e(TAG, "Error in * Query " + e);

                }

            } else {
                Log.d(TAG, "Querying " + selection);
                try {

                    if (localMap.containsKey(selection)) {
                        MatrixCursor.RowBuilder mRowCursor = mCursor.newRow();
                        mRowCursor.add(columnName[0], selection);
                        mRowCursor.add(columnName[1], localMap.get(selection));
                    } else {
                        try {
                            String selectionHash = genHash(selection);
                            String portToQuery_t = partitionIdentifier(selectionHash);
                            String[] portToQuery = portToQuery_t.split(":");
                            String replicatedQuery = portToQuery[0] + ":" + portToQuery[1] + ":" + portToQuery[2] + ":" + selection;
                            String filesRcx = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERYREQ", replicatedQuery).get();
                            MatrixCursor.RowBuilder mRowCursor = mCursor.newRow();
                            String[] fileKeyValSplitter = filesRcx.split(":");
                            mRowCursor.add(columnName[0], fileKeyValSplitter[0]);
                            mRowCursor.add(columnName[1], fileKeyValSplitter[1]);

                        } catch (Exception e) {
                            Log.e(TAG, "EXCEPTION IN RANDOM QUERY INSIDE CLINET " + e);
                        }
                    }
                } catch (Exception e) {
                    Log.e(TAG, "Error In Random Query Hashing");
                }
            }
            return mCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

// ****************** SERVER TASKS **********************
	private class ServerTask extends AsyncTask<ServerSocket,String,Void>{

	@Override
	protected Void doInBackground(ServerSocket... serverSockets) {
		ServerSocket serverSocket = serverSockets[0];

		// REPORTING TO PORT LEADER 5554
		try {
			while (true) {
				Socket clientSocket = serverSocket.accept();
//				Log.d(TAG,"RECEIEVING MESGS FROM CLIENTS");
                InputStream inputStream = clientSocket.getInputStream();
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                ArrayList<String> msgRecieved = (ArrayList<String>) objectInputStream.readObject();
                Log.d(TAG, "KEY-VALUE INSERT REQ FROM CLIENT IS -- " + msgRecieved);
                String[] msgCheck = msgRecieved.get(0).split(":");
                if (msgCheck[0].equals("Insert")){
                    String[] insert_iter = msgRecieved.get(0).split(":");
                    //OVERWRITE
                    localMap.put(insert_iter[1],insert_iter[2]);
                }
                else if(msgCheck[0].equals("SENDALL")){
                    Log.d(TAG,"RCX SEND ALL REQUEST");
                    OutputStream outputStream = clientSocket.getOutputStream();
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                    objectOutputStream.writeObject(localMap);
                }

                else if(msgCheck[0].equals("QUERYREQ")){
                    Log.d(TAG,"RECEIVED QUERY REQ FROM A CLIENT....." + msgCheck[1]);
                    String queryToFind = msgCheck[1];
                    String valueOfKey = localMap.get(queryToFind);
                    String tempString = queryToFind + ":" + valueOfKey;
                    ArrayList<String> queryBuilder = new ArrayList<String>();
                    queryBuilder.add(tempString);
                    OutputStream outputStream = clientSocket.getOutputStream();
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                    objectOutputStream.writeObject(queryBuilder);
                }

                else if(msgCheck[0].equals("RECOVERY")){
                    Log.d(TAG,"RECIEVED RECIVERY MSG FROM A CLIENT....");
                    OutputStream outputStream = clientSocket.getOutputStream();
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                    objectOutputStream.writeObject(localMap);
                }

			}
		}catch(NullPointerException e){
			Log.e(TAG,"NULL POINTED EXCEPTION CAUGHT " + e );
		}catch(Exception e){
			Log.e(TAG,"Exception in Server Side Code " + e);
		}

		return null;
	}
}


 // ************* CLIENT TASK - PORTS REGISTERING TO LEADER ***********
	private class ClientTask extends  AsyncTask<String,Void,String>{

	 @Override
	 protected String doInBackground(String... keyValRcx) {

			try {
                if(keyValRcx[0].equals("QUERYREQ")) {
                    Log.d(TAG, "SENT QUERY REQ TO SERVERS....." + keyValRcx[1]);
                    String[] portsToQuery = keyValRcx[1].split(":");
                    String[] queryPortsList = {portsToQuery[0],portsToQuery[1],portsToQuery[2]};
                    for (String queryPorts : queryPortsList) {
                        try {
                            String portToForwardQuery = Integer.toString(Integer.parseInt(queryPorts) * 2);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(portToForwardQuery));
                            ArrayList<String> queryReqToSend = new ArrayList<String>();
                            queryReqToSend.add("QUERYREQ" + ":" + portsToQuery[3]);
                            Log.d(TAG, "GENERATED QUERY REQ " + queryReqToSend);
                            OutputStream outputStream = socket.getOutputStream();
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                            objectOutputStream.writeObject(queryReqToSend);
                            outputStream.flush();

                            InputStream inputStream = socket.getInputStream();
                            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                            ArrayList<String> requestedQueryRcx = ((ArrayList<String>) objectInputStream.readObject());
//                        Log.d(TAG,"FILES RECIEVED FROM SERVER " + requestedQueryRcx);
                            queryToReturn = requestedQueryRcx.get(0);
                        } catch (Exception e) {

                        }
                    }
                }
                else {
//                    Log.d("KeyVal Recieved", "Key and Value TO send is " + keyValRcx[0] + "  To Port " + keyValRcx[1]);
                    String portRcx = keyValRcx[1];
                    String[] tempPortSplit = portRcx.split(":");

                    for(String replicationPorts: tempPortSplit){
                        try {
//                        Log.d(TAG,"REPLICATION DONE FROM " + globalPort + " TO  -- " + replicationPorts);
                            String portToForward = Integer.toString(Integer.parseInt(replicationPorts) * 2);
                            serverDownCheck = portToForward;
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(portToForward));
                            ArrayList<String> msgToSend = new ArrayList<String>();
                            msgToSend.add("Insert" + ":" + keyValRcx[0]);
                            OutputStream outputStream = socket.getOutputStream();
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                            objectOutputStream.writeObject(msgToSend);
                            Log.d(TAG, "KEY-VALUE SENT TO SERVER FOR INSERT --" + msgToSend + "  TO --- " + keyValRcx[1] + "  ROUTED TO " + portToForward + " --" + replicationPorts);
                            outputStream.flush();
                        }
                        catch (Exception e){

                        }

                    }
                }
			} catch (Exception e) {
				Log.e(TAG, "Error in Port Registering Client task " + e);
                serverDownConfirmation = serverDownCheck;
                Log.d(TAG,"SERVER " + serverDownConfirmation + " DOWN DOWN!!!!");
			}
		 return queryToReturn;
	 }
 }

    private class  RecoveryTask extends AsyncTask<String,Void,Void>{

        @Override
        synchronized protected Void doInBackground(String... failMsg) {
                String[] portsToReq = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
                try {
                    for (String recoveryPorts : portsToReq) {
                        if (!recoveryPorts.equals(failMsg[1])) {
                            Log.d(TAG, "SENDING RECOVERY MSG TO SERVERS");
//                        Thread.sleep(50);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(recoveryPorts));
                            ArrayList<String> reqToSend = new ArrayList<String>();
                            reqToSend.add("RECOVERY" + ":" + failMsg[1]);
                            OutputStream outputStream = socket.getOutputStream();
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                            objectOutputStream.writeObject(reqToSend);
                            outputStream.flush();

                            InputStream inputStream = socket.getInputStream();
                            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                            HashMap<String, String> queryFilesReceieved = (HashMap<String, String>) objectInputStream.readObject();
                            Log.d(TAG, "QUERY FILES RCX  " + queryFilesReceieved);
                            if (queryFilesReceieved.isEmpty()) {
                                Log.d(TAG, "NO RECOVERY MAP NO FAILURE AT THIS SERVER");
                                break;
                            } else {
                                for (ConcurrentHashMap.Entry<String, String> items : queryFilesReceieved.entrySet()) {
                                    String keyToInsert = items.getKey();
                                    String key_hash = genHash(keyToInsert);
                                    String replitem = partitionIdentifier(key_hash);
                                    String[] repl_ports = replitem.split(":");
                                    if (repl_ports[0].equals(globalPort)) {
                                        Log.d("INSERTED", "INSERTED VALUES " + items.getKey() + "=" + items.getValue() + "    -- " + replitem);
                                        String valToInsert = items.getValue();
//                                    if(!replicatedMap.containsKey(keyToInsert)) {
                                        replicatedMap.put(keyToInsert, valToInsert);
//                                    }
                                    } else if (repl_ports[0].equals(predecessor_1)) {
                                        Log.d("INSERTED", "INSERTED VALUES " + items.getKey() + "=" + items.getValue() + "    -- " + replitem);
                                        String valToInsert = items.getValue();
//                                    if(!replicatedMap.containsKey(keyToInsert)) {
                                        replicatedMap.put(keyToInsert, valToInsert);
//                                    }
                                    } else if (repl_ports[0].equals(predecessor_2)) {
                                        Log.d("INSERTED", "INSERTED VALUES " + items.getKey() + "=" + items.getValue() + "    -- " + replitem);
                                        String valToInsert = items.getValue();
//                                    if(!replicatedMap.containsKey(keyToInsert)) {
                                        replicatedMap.put(keyToInsert, valToInsert);
//                                    }
                                    }

                                    else {
                                        Log.d("MISSED", "MISSED OUT VALUES  " + items.getKey() + "=" + items.getValue() + "    -- " + replitem);
                                        continue;
                                    }
                                }
                            }
                            localMap.putAll(replicatedMap);
//                        globalFilesMap.putAll(localMap);
                        } else {
                            Log.d(TAG, "I CANNOT SEND RECOVERY");
                            continue;
                        }
                    }
                    //if(!replicatedMap.isEmpty()){

                    //}
                    Log.d("LOCAL MAP CHECK", "LOCAL MAP SHOULD BE EMPTY HERE" + localMap);
                } catch (Exception e) {
                    Log.e(TAG, "EXCEPTION IN RECOVERY TASK " + e);
                }
                return null;
        }
    }

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
