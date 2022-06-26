package userManagement;

import java.io.File;
import java.util.LinkedList;

import com.opensymphony.xwork2.ActionSupport;

import database.*;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class FileDownloader extends ActionSupport{
	
	private	static final long serialVersionUID = 1L;
	private String path;
	private String name;
	private String newname;
	private String whose;
	//用来返回结果给前端
    private	String	result;
	private JSONObject devices;
	private String fileType;
	private int fileSize;
	private int noa; // 文件本身的切片数量
	private int nod; // 文件切片通过纠删码产生额外切片的数量
	//TODO
	private static final String fragmentFolderPath = "/usr/local/tomcat/webapps/DFS/CloudDriveServer/downloadFragment";
	private static final String fileFolderPath = "/usr/local/tomcat/webapps/DFS/CloudDriveServer/tmpFile";
	
	public String getNewname()
	{
		return this.newname;
	}

	public void setNewname(String newname)
	{
		this.newname = newname;
	}

	public String getWhose()
	{
		return this.whose;
	}

	public void setWhose(String whose)
	{
		this.whose = whose;
	}

    public	String	getPath()
    {
    	return this.path;
    }
    
	public void setPath(String path)
	{
		this.path = path;
	}
	
    public	String	getResult()
    {
    	return this.result;
    }
    
	public void setResult(String result)
	{
		this.result = result;
	}
	
    public	String	getName()
    {
    	return this.name;
    }
    
	public void setName(String name)
	{
		this.name = name;
	}

	private JSONObject test;

	public	JSONObject	getTest()
	{
		return this.test;
	}

	public void setTest(String name)
	{
		this.name = name;
	}


	public	JSONObject	getDevices()
	{
		return this.devices;
	}

	public void setDevices(JSONObject devices)
	{
		this.devices = devices;
	}

	public	String	getFileType()
	{
		return this.fileType;
	}

	public void setFileType(String fileType)
	{
		this.fileType = fileType;
	}
	public	int	getFileSize()
	{
		return this.fileSize;
	}

	public void setFileSize(int fileSize)
	{
		this.fileSize = fileSize;
	}

	public	int	getNoa()
	{
		return this.noa;
	}

	public void setNoa(int noa)
	{
		this.noa = noa;
	}

	public	int	getNod()
	{
		return this.nod;
	}

	public void setNod(int nod)
	{
		this.nod = nod;
	}

	
	public String downloadRegister(){
		//return -1 if error
		//return 0 if can not collect enough fragments
		//else, return 1
		
		System.out.println("downloadRegister is called");
		
		Query query=new Query();

		// 查询指定路径下是否有某个名字的文件
		FileItem fileItem=query.queryFile(path, name);

		// 查询在线的所有设备组成的数组
		DeviceItem onlineDevice[]=query.queryOnlineDevice();

		
		if(onlineDevice==null)
		{
			System.out.println(1);
			result = "NotEnoughFragments";
			/*
			JSONObject responseDetailsJson = new JSONObject();
			JSONArray jsonArray = new JSONArray();

			JSONObject formDetailsJson = new JSONObject();
			formDetailsJson.put("id", "1");
			formDetailsJson.put("name", "name1");
			jsonArray.add(formDetailsJson);
			formDetailsJson = new JSONObject();
			formDetailsJson.put("id", "2");
			formDetailsJson.put("name", "name2");
			jsonArray.add(formDetailsJson);

			responseDetailsJson.put("forms", jsonArray);
			test=responseDetailsJson;
			System.out.println(test);*/
			return "success";
		}

		// 没找到符合要求的文件
		if (fileItem==null || fileItem.getNod()<1){
			query.closeConnection();
			result = "Error";
			return "success";
		}		
		else{
			// 切片的数量
			int nod=fileItem.getNod();
			
			// 纠删码扩展碎片的数量
			int noa=fileItem.getNoa();

			int id=fileItem.getId();
			int deviceID;
			String str;
			// 这里的 reqItems 在后面并没有用到
			LinkedList<AnotherRequestItem> reqItems = new LinkedList<>();
			JSONArray jsonArray = new JSONArray();

			// 查找每个碎片所在的设备 ID ，并把这些设备的信息存储在 jsonArray 里
			for (int i=0;i<nod+noa;i++){
				
				// 查询相应的文件碎片
				str=query.queryFragment(id*100+i);

				// str1.equals(anObject: str2) 用于查询 str1 和 str2 的内容是否相等
				if (str==null || str.equals("-1"))
					continue;

				// 将 String 类型转化成整型
				deviceID=Integer.parseInt(str);

				for (DeviceItem deviceItem : onlineDevice){//TODO
					if (deviceItem.getId()==deviceID){
						// 找到这个文件所在的设备
						DeviceItem curDevice=query.queryDevice(deviceID);
						//reqItems.add(new AnotherRequestItem(curDevice.getIp(), String.valueOf(curDevice.getPort()), String.valueOf(id*100+i),fileType,i));

						JSONObject formDetailsJson = new JSONObject();
						formDetailsJson.put("filename", String.valueOf(id*100+i));
						formDetailsJson.put("fragmentId", i);
						formDetailsJson.put("ip", curDevice.getIp());
						formDetailsJson.put("port", String.valueOf(curDevice.getPort()));
						jsonArray.add(formDetailsJson);
						// jsonArray 存储设备的相关信息
						break;
					}
				}
			}

			if (jsonArray.size() < nod){
				query.closeConnection();
				result = "NotEnoughFragments";
				return "success";
			}
			else{

				System.out.println(reqItems.size());

				//System.out.println(jsonArray.size());
				//System.out.println(jsonArray.toString());
				devices= new JSONObject();

				// devices.forms 内就是该文件切分成的碎片所在的存储设备信息
				// put 用于新增 JSON 对象的 key
				devices.put("forms", jsonArray);
				System.out.println(devices);

				this.fileSize= fileItem.getFileSize();
				this.fileType= fileItem.getFileType();
				this.nod=fileItem.getNod();
				this.noa=fileItem.getNoa();
				query.closeConnection();
				result = "OK";
				return "success";
			}
		}
	}
	
	public String progressCheck(){
		//return -1 if error
		//else, return a number from 0 to 100 as # of fragments which have been downloaded
		Query query=new Query();
		FileItem fileItem=query.queryFile(path, name);
		query.closeConnection();
		if (fileItem==null)
		{
			result = "Error";
			return "success";
		}
		else{
			String fileId=Integer.toString(fileItem.getId());
			int collectedFiles = 0;
			File dir=new File(fragmentFolderPath);			
			String files[]=dir.list();
			for (String file : files){
				if (file.substring(0, file.length()-2).equals(fileId))
					collectedFiles++;
			}
			float percentage = (float)collectedFiles / fileItem.getNoa();
			//只需要总数目的一半　　因此进度×２
			percentage *= 2;
			collectedFiles = (int) (percentage * 100);
			System.out.println("pregress check is called,return "+ collectedFiles);
			
			result = Integer.toString(collectedFiles);
			return "success";
		}		
	}
	

	public String renameRegister(){
		System.out.println("renameRegister is called");

		Query query = new Query();

		boolean flag = query.renameFile(path, name, newname);

		if(flag){
			result = "success";
			return "success";
		}
		else{
			result = "failure";
			return "success";
		}
	}

	public String adddirRegister(){
		Query query = new Query();

		boolean flag = query.queryDir(whose, name, path); //查询是否存在这个目录

		if(flag){ //说明文件夹已经存在
			result = "Failure: the dir already exists.";
			return "success";
		}

		// 目录不存在，可以新建该目录 

		Query query1 = new Query();

		flag = query1.addDir(whose, name, path);
		if(!flag){
			//result = "Failure: dir add error";
			result = whose + " " + name + " " + path; 
			return "success";
		}

		result = "Success";
		return "success";
	}

	public String deleteRegister(){
		System.out.println("deleteRegister is called");

		Query query = new Query();

		
		FileItem fileItem = query.queryFile(path, name);

		int nod = fileItem.getNod();  // 获取 division 的数量

		int noa = fileItem.getNoa();  // 获取 append 的数量

		int id = fileItem.getId();  // 获取文件的 id

		boolean frag_flag = true;
		int fail_id = -1;
		for(int i=0; i<nod+noa; i++){  // 删除相应的文件碎片
			Query query1 = new Query();
			frag_flag = query1.deleteFragment(id*100+i);
			if(!frag_flag) {
				fail_id = id*100 + i;
				break;
			}
		}

		boolean flag=query.deleteFile(path, name);
		if(flag && frag_flag){
			result = "success";
			return "success";
		}
		else if(!frag_flag){
			result = "failure" + fail_id;
			return "success";
		}
		else{
			result = "failure";
			return "success";
		}
	}
	
	
	public String decodeFile(){
		//return 1 and DELETE ALL FRAGMENTS OF INPUT FILE if decode successfully
		//else, return 0
		
		System.out.println("decodeFile is called");
		
		Query query=new Query();
		FileItem fileItem=query.queryFile(path, name);
		query.closeConnection();
		if (fileItem==null)
		{
			result = "Error";
			return "success";
		}
		else{
			try {
				if (com.backblaze.erasure.Decoder.decode(
						new File(fragmentFolderPath), new File(fileFolderPath+'/'+name), 
						fileItem.getId(), fileItem.getNoa())) {
					File dir=new File(fragmentFolderPath);			
					File files[]=dir.listFiles();
					String fileId=Integer.toString(fileItem.getId());
					String str;
					for (File file : files){
						str=file.getName();
						if (str.substring(0, str.length()-2).equals(fileId))
							file.delete();
					}	
				{
					result = "OK";
					return "success";
				}
			}			
			else
			{
				result = "Error";
				return "success";
			}
			}
			catch (Exception e) {
					result = "Error";
					return "success";
			}
		}		
	}
	
	/* only for debug 
	public static void main(String args[]) {

     	System.out.println(downloadRegister("TIM/", "2016.jpg"));
      	System.out.println(downloadRegister("TIM/tmp/", "2015.jpg"));
      	System.out.println(downloadRegister("TIM/", "2015.jpg"));
      	
      	System.out.println(progressCheck("TIM/", "2016.jpg"));
      	System.out.println(progressCheck("TIM/tmp/", "2015.jpg"));
      	System.out.println(progressCheck("TIM/", "2015.jpg"));
      	
      	System.out.println(decodeFile("TIM/", "2016.jpg"));
      	System.out.println(decodeFile("TIM/tmp/", "2015.jpg"));
      	
	}
	*/
}
