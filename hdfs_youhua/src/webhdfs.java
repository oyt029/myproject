

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;

import org.openqa.selenium.htmlunit.HtmlUnitDriver;

public class webhdfs {

	private String hdfsurl = "http://192.168.1.169:50070/webhdfs/v1/";
	private StringBuilder sb = new StringBuilder(64);
	private String s = null;

	public HttpURLConnection getConnection(String method, String hdfsRest,
			String path, String parameters) {
		HttpURLConnection con = null;
		URL url = null;
		try {
			sb.setLength(0);
			sb.append(hdfsurl);
			sb.append(path);
			sb.append("?op=");
			if (hdfsRest == null) {
				hdfsRest = "GETFILESTATUS";
			}
			sb.append(hdfsRest);
			if (parameters != null) {
				sb.append(parameters);
			}
			url = new URL(sb.toString());
			con = (HttpURLConnection) url.openConnection();
			if (method == null) {
				method = "GET";
			}
			con.setRequestMethod(method);
			con.setRequestProperty("accept", "*/*");
			con.setRequestProperty("connection", "Keep-Alive");
			s = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)";
			con.setRequestProperty("User-Agent", s);
			con.setRequestProperty("Accept-Encoding", "gzip");
			con.setDoInput(true);
			con.setDoOutput(true);
			con.setUseCaches(true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return con;
	}

	public String getResult(String method, String hdfsRest, String path) {
		HttpURLConnection con = null;
		try {
			con = getConnection(method, hdfsRest, path, null);
			if (con != null) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						con.getInputStream(), "UTF-8"));
				sb.setLength(0);
				while ((s = br.readLine()) != null) {
					sb.append(s);
					sb.append("\r\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (con != null) {
				con.disconnect();
			}
		}
		return sb.toString();
	}

	public String getResult(HttpURLConnection con) {
		try {
			if (con != null) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						con.getInputStream(), "UTF-8"));
				sb.setLength(0);
				while ((s = br.readLine()) != null) {
					sb.append(s);
					sb.append("\r\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (con != null) {
				con.disconnect();
			}
		}
		return sb.toString();
	}

	public String create(String filepath) {
		return create(filepath, false, -1, null, null, 0);
	}

	public String create(String filepath, boolean overwrite, long blocksize,
			String replication, String permission, int buffersize) {
		sb.setLength(0);
		if (overwrite) {
			sb.append("&overwrite=");
			sb.append(overwrite);
		}
		if (blocksize > -1) {
			sb.append("&blocksize=");
			sb.append(blocksize);
		}
		if (replication != null) {
			sb.append("&replication=");
			sb.append(replication);
		}
		if (permission != null) {
			sb.append("&permission=");
			sb.append(permission);
		}
		if (buffersize > 0) {
			sb.append("&buffersize=");
			sb.append(buffersize);
		}
		HttpURLConnection con = getConnection("PUT", "CREATE", filepath, sb
				.toString());
		return getResult(con);
	}

	public String createAndWrite(String filepath, String content) {
		HttpURLConnection con = getConnection("PUT", "CREATE", filepath, null);
		if (con != null) {
			try {
				con.getOutputStream().write(content.getBytes());
				con.getOutputStream().flush();
				return getResult(con);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public String cancat(String filepath, String paths) {
		sb.setLength(0);
		sb.append("&sources=");
		sb.append(paths);
		HttpURLConnection con = getConnection("POST", "CONCAT", filepath, sb
				.toString());
		return getResult(con);
	}

	public String append(String filepath, String content) {
		HttpURLConnection con = getConnection("POST", "APPEND", filepath, null);
		if (con != null) {
			try {
				con.getOutputStream().write(content.getBytes());
				con.getOutputStream().flush();
				
				BufferedInputStream oBIn = null;
				RandomAccessFile oRAFile = null;
				oBIn = new BufferedInputStream(con.getInputStream());
				while (oBIn.read() > -1) {
					
				}
				
				//return getResult(con);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public String delete(String filepath) {
		return getResult("DELETE", "DELETE", filepath);
	}

	public String open(String filepath) {
		return getResult("GET", "OPEN", filepath);
	}

	public String mkdirs(String filepath) {
		return getResult("PUT", "MKDIRS", filepath);
	}

	public String rename(String filepath, String destination) {
		sb.setLength(0);
		sb.append("&destination=");
		sb.append(destination);
		HttpURLConnection con = getConnection("PUT", "RENAME", filepath, sb
				.toString());
		return getResult(con);
	}

	public String status(String filepath) {
		return getResult("GET", "GETFILESTATUS", filepath);
	}

	public String list(String filepath) {
		return getResult("GET", "LISTSTATUS", filepath);
	}

	public String getDirectorySummary(String filepath) {
		return getResult("GET", "GETCONTENTSUMMARY", filepath);
	}

	public String getCheckSum(String filepath) {
		return getResult("GET", "GETFILECHECKSUM", filepath);
	}

	public String getHomeDirectory(String filepath) {
		return getResult("GET", "GETHOMEDIRECTORY", filepath);
	}

	public String setPermission(String filepath) {
		return getResult("PUT", "SETPERMISSION", filepath);
	}

	public String setOwner(String filepath, String owner, String group) {
		sb.setLength(0);
		sb.append("&owner=");
		sb.append(owner);
		sb.append("&group=");
		sb.append(group);
		HttpURLConnection con = getConnection("PUT", "SETOWNER", filepath, sb
				.toString());
		return getResult(con);
	}

	public String setReplicationFactor(String filepath) {
		return getResult("PUT", "SETREPLICATION", filepath);
	}

	public void search() {
		HtmlUnitDriver driver = null;
		String url = "http://192.168.1.169:50070/webhdfs/user/?op=APPEND";
		try {
			driver = new HtmlUnitDriver();
			driver.get(url);
			System.out.println(driver.getCurrentUrl());
			String pageSource = driver.getPageSource();
			System.out.println(pageSource);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (driver != null) {
				driver.close();
			}
		}
	}

	public static void main(String[] args) {
		webhdfs webhdfs = new webhdfs();
		String filepath = null;
		filepath = "app-logs";
		String s = null;
		// s = webhdfs.append(filepath, "aaaaaaaaaaa");
		 //s = webhdfs.list(filepath);
		 s = webhdfs.status(filepath);
		 System.out.println(s);
		
		//webhdfs.search();
	}

}
