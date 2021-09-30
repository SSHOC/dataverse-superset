/*
 * The MIT License
 * 
 * Copyright (c) 2021 SSHOC Dataverse
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package eu.sshoc.dataversesuperset;

import eu.sshoc.dataversesuperset.DataInfo.Status;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Controller
public class WebController {
	
	@Autowired
	private Logger logger;
	
	@Autowired
	private Map<String, String> siteUrlMapping;
	
	@Autowired
	private DataLoader dataLoader;
	@Autowired
	private DataSaver dataSaver;
	@Autowired
	private Superset superset;
	@Autowired
	private ApplicationContext appContext;
	
	@GetMapping("/dataverse-superset")
	public String displayCsv(Model model, HttpSession session,
			@RequestParam(value = "siteUrl", required = false) String siteUrl,
			@RequestParam(value = "fileid", required = false) String fileId,
			@RequestParam(value = "fileUrl", required = false) String fileUrl) throws IOException {
		@SuppressWarnings("unchecked")
		Map<String, DataInfo> sessionDataInfos = (Map<String, DataInfo>) session.getAttribute("dataInfos");

		if (sessionDataInfos == null && (siteUrl == null || fileId == null) && fileUrl == null) {
			throw new NullPointerException();
		}

		if (sessionDataInfos == null) {
			sessionDataInfos = new HashMap<>();
			session.setAttribute("dataInfos", sessionDataInfos);
		}

		if (siteUrl != null && fileId != null) {
			siteUrl = siteUrlMapping.getOrDefault(siteUrl, siteUrl);
			fileUrl = siteUrl + "/api/access/datafile/" + fileId;
		}

		String name = DataInfo.getName(fileUrl);
		DataInfo dataInfo = sessionDataInfos.get(name);
		if (dataInfo == null) {
			dataInfo = new DataInfo(siteUrl, fileId, fileUrl);
			long datasetId = superset.findDataset(name);
			if (datasetId >= 0) {
				dataInfo.datasetId = datasetId;
				dataInfo.status = Status.COMPLETE;
			}
			dataLoader.loadMetadata(dataInfo);
			sessionDataInfos.put(name, dataInfo);
		}
		model.addAttribute("data", dataInfo);
		
		return "main";
	}
	
	@PostMapping("/dataverse-superset")
	public String importCSV(HttpSession session, RedirectAttributes redirectAttributes,
			@RequestParam String datasetName) {
		@SuppressWarnings("unchecked")
		Map<String, DataInfo> sessionDataInfos = (Map<String, DataInfo>) session.getAttribute("dataInfos");
		DataInfo dataInfo = sessionDataInfos.get(datasetName);
		dataInfo.status = Status.IN_PROGRESS;
		appContext.getBean(WebController.class).importCsvAsync(dataInfo);
		redirectAttributes.addAttribute("siteUrl", dataInfo.siteUrl);
		redirectAttributes.addAttribute("fileid", dataInfo.fileId);
		redirectAttributes.addAttribute("fileUrl", dataInfo.fileUrl);
		return "redirect:/dataverse-superset";
	}
	
	@Async
	public void importCsvAsync(DataInfo dataInfo) {
		String tableName = null;
		try {
			tableName = dataSaver.createTable(dataInfo);
			dataInfo.datasetId = superset.createDataset(tableName);
			dataInfo.status = Status.COMPLETE;
		} catch (Exception e) {
			logger.error("could not load " + dataInfo.fileUrl, e);
			dataInfo.error = e.getMessage();
			dataInfo.status = Status.ERROR;
			if (tableName != null)
				dataSaver.deleteTable(tableName);
		}
	}
}
