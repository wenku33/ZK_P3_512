/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2024, Bettina Kemme; ©2025, Olivier Michaud
*/
import java.io.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;

// TODO
// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your manager process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your manager's logic and worker's logic.
//		This is important as both the manager and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For simplicity, so far all the code in a single class (including the callbacks).
//		You are free to break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! Managers and Workers are also clients of ZK and the ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		In particular, if the process is a worker, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback
{
    ZooKeeper zk;
    String zkServer, pinfo;
    boolean isManager=false;
    boolean initialized=false;

    final String distPath = "/dist11"; 
    final String tasksPath = distPath + "/tasks";
    final String workersPath = distPath + "/workers";
    final String assignPath = distPath + "/assign";
    final String managerPath = distPath + "/manager";

    ExecutorService workerExecutor = Executors.newCachedThreadPool();

    DistProcess(String zkhost)
    {
        zkServer=zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
    {
        zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.

        // Weird connection bug. Manager ended up in workers
        try{
            System.out.println("Empty out workers");
            List<String> workers = zk.getChildren(workersPath, false);
            // Mngr exists
            if (zk.exists(managerPath , false) != null) {
                String mngr = new String(zk.getData(managerPath, false, null));
                if(workers.contains(mngr)){
                    zk.delete(workers +"/"+mngr, -1);
                }
            } else {
                for (String w : workers) {
                    zk.delete(workersPath+"/"+w, -1);
                }
            }
            
        } catch (Exception e) {
            System.out.println("Something went wrong resetting the workers");
            e.printStackTrace();
        }
    }

    void initialize()
    {
        try
        {
            System.out.println("Manager doesn't exist " + (zk.exists("/dist11/manager", false) == null));
            runForManager();	// See if you can become the manager (i.e, no other manager exists)
            isManager=true;
            setupManager();
            List<String> workers = zk.getChildren(workersPath, false);
            // System.out.println("has mngr been added to children? "+workers.get(0));
            System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isManager?"manager":"worker"));
        }catch(NodeExistsException nee)
        { 
            isManager=false; 
            // setupWorker();
            try {  
                setupWorker();
                System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isManager?"manager":"worker"));

            }catch(Exception e){
                System.out.println(e);
            }
            
        } // TODO: What else will you need if this was a worker process?
        catch(UnknownHostException uhe)
        { System.out.println(uhe); }
        catch(KeeperException ke)
        { System.out.println(ke); }
        catch(InterruptedException ie)
        { System.out.println(ie); }


    }

    void setupManager() throws KeeperException, InterruptedException {
        createIfNotExists(distPath, new byte[0]);
        createIfNotExists(tasksPath, new byte[0]);
        createIfNotExists(workersPath, new byte[0]);
        createIfNotExists(assignPath, new byte[0]);

        zk.getChildren(workersPath, this, this, null);

        // watch tasks list
        zk.getChildren(tasksPath, this, this, null);

        System.out.println("DISTAPP : Manager watching tasks and workers");
    }


    void setupWorker() throws KeeperException, InterruptedException {
        createIfNotExists(distPath, new byte[0]);
        createIfNotExists(tasksPath, new byte[0]);
        createIfNotExists(workersPath, new byte[0]);
        createIfNotExists(assignPath, new byte[0]);
        
        String myWorkerPath = workersPath + "/" + pinfo;
        zk.create(myWorkerPath, "idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        
        // try {
        //     zk.create(myWorkerPath, "idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        // } catch (KeeperException.NodeExistsException nee) {
        //     // if leftover node exists (shouldn't happen if no crash), delete and re-create
        //     zk.delete(myWorkerPath, -1);
        //     zk.create(myWorkerPath, "idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);            
        // }

        // watch my assign node (existence)
        watchMyAssignment();

        System.out.println("DISTAPP : Worker registered: " + myWorkerPath);
    }

    void createIfNotExists(String path, byte[] data) {
        try {
            if (zk.exists(path, false) == null) {
                zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException ignore) {
        } catch (Exception e) {
            System.err.println("DISTAPP : createIfNotExists error for " + path + " : " + e);
        }
    }

    void watchMyAssignment() {
        String myAssign = assignPath + "/" + pinfo;
        try {
            // watch for create/delete on this node. Currently, not yet created
            zk.exists(myAssign, this); 
        } catch (Exception e) {
            System.err.println("DISTAPP : watchMyAssignment error: " + e);
        }
    }

    // Try to become the manager.
    void runForManager() throws UnknownHostException, KeeperException, InterruptedException
    {
        //Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
        // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        zk.create("/dist11/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void process(WatchedEvent e)
    {
        //Get watcher notifications.

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library 
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        System.out.println("DISTAPP : Event received : " + e);

        if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
        {
            // Once we are connected, do our intialization stuff.
            if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initialized == false) 
            {
                initialize();
                initialized = true;
            }
            // Is this okay?
            return;
        }

        // Manager should be notified if any new znodes are added to tasks.
        if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath()!=null)
        {   
            try{
                List<String> workers = zk.getChildren(workersPath, false);
                // System.out.println("has mngr been added to children? When new task "+workers.get(0)); 
            }catch(Exception exo) {}
            
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the children.
            if (e.getPath().equals(tasksPath)) {
                // re-install children watch and trigger processResult (async)
                zk.getChildren(tasksPath, this, this, null);
                return;
            } else if (e.getPath().equals(workersPath)) {
                // re-install children watch to monitor worker list
                zk.getChildren(workersPath, this, this, null);
                return;
            }
        }

        if (e.getType() == Watcher.Event.EventType.NodeCreated && e.getPath() != null) {
            try{
                List<String> workers = zk.getChildren(workersPath, false);
            }catch(Exception exo) {}
            
            String myAssign = assignPath + "/" + pinfo;
            if (e.getPath().equals(myAssign)) {
                onAssignmentAppeared();
                return;
            }
        }

        
        if (e.getType() == Watcher.Event.EventType.NodeDeleted && e.getPath() != null) {
            String myAssign = assignPath + "/" + pinfo;
            if (e.getPath().equals(myAssign)) {
                // There's a finally in onAssignmentAppeared() that reinstates anyways
                // watchMyAssignment();
                // Check for more jobs
                
                // try{
                //     if(isManager) {
                //         List<String> taskChildren = zk.getChildren(tasksPath, false);
                //         startAssignment(taskChildren);
                //     }
                // } catch(Exception ex) {ex.printStackTrace();}
                
                return;
            }
        }
    }

    // Worker task execution 
    void onAssignmentAppeared() {
        final String myAssign = assignPath + "/" + pinfo;
        // To watch for myAssign Deletion, to trigger more task assignments if necessary.
        watchMyAssignment();
        workerExecutor.submit(() -> {
            try {
                // Read assign data
                byte[] assignData = zk.getData(myAssign, false, null);
                // if (assignData == null) {
                //     System.out.println("Somehow assign node has no data (no job).");
                //     watchMyAssignment();
                //     return;
                // }
                String payload = new String(assignData);
                // payload = taskNodeName::base64(serialized)
                int sep = payload.indexOf("::");
                // if (sep == -1) {
                //     System.err.println("DISTAPP : Invalid assign payload: " + payload);
                //     // cleanup & rearm
                //     try { zk.delete(myAssign, -1); } catch(Exception ex){}
                //     zk.setData(workersPath + "/" + pinfo, "idle".getBytes(), -1);
                //     watchMyAssignment();
                //     return;
                // }
                String taskNodeName = payload.substring(0, sep);
                String base64 = payload.substring(sep + 2);
                byte[] taskSerial = Base64.getDecoder().decode(base64);

                // Deserialize DistTask
                ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
                ObjectInputStream ois = new ObjectInputStream(bis);
                DistTask dt = (DistTask) ois.readObject();

                // Run compute (heavy work) off the watcher thread
                System.out.println("DISTAPP : Worker " + pinfo + " computing task " + taskNodeName);
                dt.compute();

                // Write result to /tasks/<taskNodeName>/result
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(dt); oos.flush();
                byte[] resultBytes = bos.toByteArray();

                String resultPath = tasksPath + "/" + taskNodeName + "/result";
                try {
                    zk.create(resultPath, resultBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nee) {
                    // result node exists already, ignore or update
                    zk.setData(resultPath, resultBytes, -1);
                }

                // cleanup: remove assign node and mark self idle
                try { zk.delete(myAssign, -1); } catch(Exception ex){ex.printStackTrace();}
                zk.setData(workersPath + "/" + pinfo, "idle".getBytes(), -1);

                System.out.println("DISTAPP : Worker " + pinfo + " finished task " + taskNodeName);

            } catch (KeeperException.NoNodeException nne) {
                // assign gone, nothing to do.
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("DISTAPP : onAssignment error: " + e);
                e.printStackTrace();
                try {
                    zk.setData(workersPath + "/" + pinfo, "idle".getBytes(), -1);
                } catch (Exception ex) {}
            } finally {
                // re-arm watch whether or not we succeeded
                watchMyAssignment();
            }
        });
    }

    //Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children)
    {

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library 
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        // This logic is for manager !!
        //Every time a new task znode is created by the client, this will be invoked.

        // TODO: Filter out and go over only the newly created task znodes.
        //		Also have a mechanism to assign these tasks to a "Worker" process.
        //		The worker must invoke the "compute" function of the Task send by the client.
        //What to do if you do not have a free worker process?
        System.out.println("\nDISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
        
        if (!isManager) {
            return;
        }

        if (path.equals(tasksPath)) {
            startAssignment(children);
            //do the assigning
        } else {
            // It's /workers
            System.out.println("DISTAPP : New worker, at path " + path + ", assign any unassigned tasks");
            try {
                List<String> taskChildren = zk.getChildren(tasksPath, false);
                startAssignment(taskChildren);
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            // get tasks children
            // do the assigning with tasks children
        }   
    }

    public void startAssignment(List<String> children) {
        for(String c: children)
        {
            System.out.println(c);
            String taskFull = tasksPath + "/" + c;
            try
            {
                //TODO There is quite a bit of worker specific activities here,
                // that should be moved done by a process function as the worker.
                
                // If result already exists, skip
                if (zk.exists(taskFull + "/result", false) != null) {
                    System.out.println("DISTAPP : Results already computed for " + c + ", skip processing");
                    continue;
                }
                // If task is already assigned, skip
                if (zk.exists(taskFull + "/isAssigned", false) != null) {
                    System.out.println(
                        "DISTAPP : Task already assigned for " + c + " to " +
                        (new String(zk.getData(taskFull + "/isAssigned", false, null))) 
                        + ", skip processing"
                    );
                    continue;
                }
                // If workers don't exist yet
                if ((zk.getChildren(workersPath, false)).isEmpty()){
                    System.out.println("DISTAPP : No workers yet to deal with " + c + ", skip processing");
                    continue;
                }

                //TODO!! This is not a good approach, you should get the data using an async version of the API.
                byte[] taskSerial = zk.getData(taskFull, false, null);

                assignTaskToWorker(c, taskSerial);
                
            }
            catch(NodeExistsException nee){System.out.println(nee);}
            catch(KeeperException ke){System.out.println(ke);}
            catch(InterruptedException ie){System.out.println(ie);}
            // catch(IOException io){System.out.println(io);}
            // catch(ClassNotFoundException cne){System.out.println(cne);}
        }
    } 


    void assignTaskToWorker(String taskNodeName, byte[] taskSerial) {
        // Do we need threads here?
        try{
            List<String> workers = zk.getChildren(workersPath, false);
            // if (!workers.isEmpty()) {continue;} 

            for (String w : workers) {
                System.out.println("Assigning for worker "+ w);
                String wPath = workersPath + "/" + w;
                byte[] data = zk.getData(wPath, false, null);
                String status = new String(data); //String status = data == null ? "idle" : new String(data);
                // Give job to the first unidle worker
                if ("idle".equals(status)) {
                    String assignNode = assignPath + "/" + w;
                    // build assign payload: taskNodeName::base64(serialized bytes)
                    String base64 = Base64.getEncoder().encodeToString(taskSerial);
                    String payload = taskNodeName + "::" + base64;
                    try {
                        // zk.create(assignNode, taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        zk.create(assignNode, payload.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        // mark worker busy
                        zk.setData(wPath, "busy".getBytes(), -1);
                        // mark task assigned
                        String taskFull = tasksPath + "/" + taskNodeName + "/isAssigned"; 
                        zk.create(taskFull, w.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        System.out.println("DISTAPP : Assigned task " + taskNodeName + " -> worker " + w);
                        return;
                    } catch (KeeperException.NodeExistsException ne) {
                        // concurrent assignment for this worker; try next
                        continue;
                    }
                }
            }
            // if no idle worker available, leave task unassigned; manager will retry on next event
            System.out.println("DISTAPP : No idle worker for task " + taskNodeName + " (will retry)");

        } catch(Exception e) {
            System.err.println("DISTAPP : assignTaskToWorker error: " + e);
            e.printStackTrace();
        }
    }


    public static void main(String args[]) throws Exception
    {
        //Create a new process
        //Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        //Replace this with an approach that will make sure that the process is up and running forever.
        // Thread.sleep(20000); 
        Object lock = new Object();
        synchronized (lock) {
            lock.wait();
        }
    }
}
