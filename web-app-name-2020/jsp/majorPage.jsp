<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>

<!-- 导入ｄａｔａｂａｓｅ支持的包 -->
<%@ page import="database.*"%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html lang="zh-CN">
<head>

<!--bootstrap和JQerry相关库-->
<script src="../js/jquery/jquery.js"></script>
<script src="../js/jquery/jquery.cookie.js"></script>
<script src="../js/bootstrap-3.3.7/js/bootstrap.min.js"></script>
<link rel="stylesheet" type="text/css" href="../js/bootstrap-3.3.7/css/bootstrap.min.css">
<!--AJAX相关js动作-->
<script src="../js/ec/object_hash.js" type="text/javascript"></script>
<script src="../js/ec/erasure.js"></script>
<script src="../js/ec/funcs.js"></script>
<script src="../js/majorPage_ajax.js"></script>
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<script src="https://www.layuicdn.com/layui/layui.js"></script>
<meta charset="utf-8" />
<link rel="stylesheet" type="text/css" href="https://www.layuicdn.com/layui/css/layui.css" />

<!-- .img-width img{MAX-WIDTH: 100%!important;HEIGHT: auto!important;width:expression(this.width > 320 ? "320px" : this.width)!important;} -->
<!--MARGIN 用于设置边框属性-->
<style type="text/css">
	.img-width {MARGIN:0; WIDTH: 100%;}
	.img-width img{MAX-WIDTH: 100%; HEIGHT:100%; width:100%;}
</style>

<script>
	var proport = 1440/560;
	layui.use('carousel', function(){
		// 轮播组件
		var carousel = layui.carousel;
		carousel.render({
			elem: '#xyycarousel',
			width: '90%',
			height: (0.9*$(window).width()/proport).toString()+'px',
			arrow: 'hover',
			anim: 'updown'
		});
	});
	$(window).resize(function () {
    	// window.location.reload();
		document.getElementById("xyycarousel").style.height = (0.9*$(window).width()/proport).toString()+'px';
		// console.log($(window).width());
	});
	/*
	window.onload = function () {
		var imgH = 0.9*$(window).width()/proport;
		// console.log($(window).width());
		$('.xyycarousel').css('height', imgH+'px');
	}
	*/
</script>

<title>DFS 分布式网盘</title>

</head>
<body>

<div class="layui-carousel layui-row" id="xyycarousel" style="margin:0px auto 50px; text-align:center; width:90%;">
	<div carousel-item class="img-width">
		<div>
			<img src="../material/pic1.jpg" alt="pic1"/>
		</div>
		<div>
			<img src="../material/pic2.png" alt="pic2"/>
		</div>
		<div>
			<img src="../material/pic3.png" alt="pic3"/>
		</div>
	</div>
</div>

<div class="layui-row">
	<div style="margin:50px auto 10px; text-align:center; width:80%;">
		<h1 style="font-family: Arial, Helvetica, sans-serif; font-size: 6rem; color: #087933;">
			DFS - Distributed FileSystem
		</h1>
	</div>
</div>

<div class="row clearfix" style="margin:50px auto 50px; width:80%;">
	<div>当前访问位置：</div>
	<ul class="breadcrumb" id = "curr_path"></ul>
</div>

<div class="layui-row pre-scrollable" style="margin: 50px auto 30px; width:90%;">
	<table class="table" id="fileCatalogTable">
		<thead>
			<tr>
				<th></th>
				<th>&emsp;&emsp;&emsp;&emsp;文件名</th>
				<th>读写权限</th>
				<th>修改时间</th>
			</tr>
		</thead>
		<tbody id="file_list_body">
			<tr class="file_list_back">
				<td></td>
				<td>
					<label>
						<input type="checkbox">&emsp;&emsp;
					</label>
					<i class="glyphicon glyphicon-folder-open"></i>&emsp;../
				</td>
				<td></td>
				<td></td>
			</tr>
			<%
				Cookie cookie = null;
				Cookie[] cookies = null;
				String username = null;
				// 获取 cookies 的数据,是一个数组
				cookies = request.getCookies();
				if( cookies != null ){
					//out.println("<h2> 查找 Cookie 名与值</h2>");
					for (int i = 0; i < cookies.length; i++){
						cookie = cookies[i];
						if (("username").equals(cookie.getName())) {
							username = cookie.getValue();
							//out.print(username);
						}
					}
				} else{
					out.println("<h2>没有发现 Cookie</h2>");
				}
			%>
			<%
				// 重定向到新地址
					if(username == null){
				String site = new String("../index.html");
				response.setStatus(response.SC_MOVED_TEMPORARILY);
				response.setHeader("Location", site); }
			%>
			<%
				int i;
				Query query = new Query();
				FileItem[] files = query.queryFileList(username, "/");
				query.closeConnection();
				if(files!=null)
				{
					for(i=0;i<files.length;i++)
					{
						out.println("<tr class='file_list_go'>");
						out.println("<td></td>");
						if(files[i].isFolder()==false)
						{
							out.println("<td>");
							out.println("<label><input type=\"checkbox\">&emsp;&emsp;</label>");
							out.println("<i class=\"glyphicon glyphicon-file\"></i>&emsp;" + files[i].getFileName() + "</td>");
						}
						else
						{
							out.println("<td>");
							out.println("<label><input type=\"checkbox\">&emsp;&emsp;</label>");
							out.println("<i class=\"glyphicon glyphicon-folder-open\"></i>&emsp;" + files[i].getFileName() + "</td>");
						}
						out.println("<td>"+files[i].getAttribute()+"</td>");
						out.println("<td>"+files[i].getTime()+"</td>");
						out.println("</tr>");
					}
				}
			%>
		</tbody>
	</table>
</div>

<div class="layui-btn-container" style="margin: 50px auto 30px; text-align:center;">
	<button class="layui-btn layui-btn-radius" type="button" id="button_download">下载</button>
	<input type="file" id="files" style="display: none" onchange="fileUpload();">
	<button class="layui-btn layui-btn-radius layui-btn-normal" type="button" id="button_upload">上传</button>
	<button class="layui-btn layui-btn-radius layui-btn-danger" type="button" id="button_delete">删除</button>
	<button class="layui-btn layui-btn-radius layui-btn-primary" type="button" id="button_rename">重命名</button>
</div>

<div style="margin:10px auto 100px; text-align:center; width:60%; font-family:Microsoft YaHei,微软雅黑,Microsoft JhengHei,华文细黑,STHeiti,MingLiu; font-size: 2.5rem;">
	<p id="statusFeedback">欢迎使用</p>
</div>

<div style="margin:0 auto; width:80%; text-align:center; font-family:Microsoft YaHei,微软雅黑,Microsoft JhengHei,华文细黑,STHeiti,MingLiu; font-size: 1.2rem;">
	<p>
		本系统由罗丽薇、邱子悦、袁一玮、余致远基于<a href="https://github.com/IngramWang/DFS_OSH2017_USTC">另一项目</a>共同开发，
		有疑问欢迎提 <a href="https://github.com/OSH-2020/x-dontpanic">issue</a>。
	</p>
</div>

<script src="../js/wasm/wasm_exec.js"></script>
<script>
	const go = new Go();
	WebAssembly.instantiateStreaming(fetch("../js/wasm/mycoder.wasm"), go.importObject)
			.then((result) => go.run(result.instance));
</script>

</body>
</html>
