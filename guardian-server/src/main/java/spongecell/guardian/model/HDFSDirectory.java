package spongecell.guardian.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.PrintSetup;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

@Slf4j
@Getter @Setter
public class HDFSDirectory {
	private String owner;
	private String pathSuffix;
	private int numChildren;
	private String statusCheck; 
	private Long accessTime;
	private Long blockSize;
	private int childrenNum;
	private int fileId; 
	private String group; 
	private int length; 
	private Long modificationTime;
	private String permission;
	private int replication;
	private int storagePolicy;
	private String type;
	private ArrayNode fileStatus;
	private String targetDir;
	private String HDFS = "HDFS";
	private String COLON = ":";

	public static final String FILE_CHECK_SUCCESS =  "success";
	public static final String FILE_CHECK_FAIL =  "fail";
	public static final String FILE_CHECK_UNKNOWN =  "unknown";
	public boolean valid;
	
	public HDFSDirectory() { 
		statusCheck = FILE_CHECK_UNKNOWN;
		valid = Boolean.TRUE;
	}
	
	public String getFileStatus() {
		StringBuffer sbuf = new StringBuffer();
		Iterator<JsonNode> nodes = fileStatus.iterator();
		while (nodes.hasNext()) {
			JsonNode node = nodes.next();
			sbuf.append(node.toString() + "\n");
		}
		log.info("*********************** \n" + sbuf.toString());
		return sbuf.toString();
	} 

	public void setTargetDir(String targetDir) {
		this.targetDir = HDFS + COLON + targetDir;
	}	
	
	public ArrayNode getRawFileStatus() {
		return fileStatus;
	}
	
	//*********************************************
	//TODO Modularize and use schema definitions.
	// Should be in a WorkBook Builder class.
	//*********************************************
	public Workbook getExcelFileStatusWorkBook() throws IOException {
		Workbook workBook = new HSSFWorkbook();
		Map<String, CellStyle> styles = createStyles(workBook);
		
		CreationHelper createHelper = workBook.getCreationHelper();
	    Sheet sheet = workBook.createSheet("HDFS File System Report");
	    
	    PrintSetup printSetup = sheet.getPrintSetup();
        printSetup.setLandscape(true);
        sheet.setFitToPage(true);
        sheet.setHorizontallyCenter(true);
	    
	    Row titleRow = sheet.createRow(0);
        titleRow.setHeightInPoints(45);
	    Cell titleCell = titleRow.createCell(0);
        titleCell.setCellValue("HDFS File System Report");
        titleCell.setCellStyle(styles.get("title"));
        sheet.addMergedRegion(CellRangeAddress.valueOf("$A$1:$M$1"));
        
	    Row targetDirRow = sheet.createRow(1);
		targetDirRow.setHeightInPoints(25);
	    Cell targetDirCell = targetDirRow.createCell(0);
        targetDirCell.setCellValue(targetDir);
        targetDirCell.setCellStyle(styles.get("eventSourceFont"));
        sheet.addMergedRegion(CellRangeAddress.valueOf("$A$2:$M$2"));
        
	    short i = 2;
		Iterator<JsonNode> nodes = fileStatus.iterator();
		while (nodes.hasNext()) {
			JsonNode node = nodes.next();
			Row row;
			if (i == 2) {
				row = sheet.createRow(i);
				Iterator<String> fields = node.fieldNames();
				int k = 0;
				while (fields.hasNext()) {
					String field = fields.next();
					row.createCell(k).setCellValue(createHelper.createRichTextString(field));
					sheet.autoSizeColumn(k);
					k++;
				}
				i++;
			}
			row = sheet.createRow(i);
			int j = 0;
			Iterator<JsonNode> values = node.elements(); 
			while (values.hasNext()) {
				JsonNode value = values.next();
				row.createCell(j).setCellValue(createHelper.createRichTextString(value.toString()));
				sheet.autoSizeColumn(j);
				j++;
			}
			i++;
		}
		return workBook;
	} 
	
	private static Map<String, CellStyle> createStyles(Workbook wb){
        Map<String, CellStyle> styles = new HashMap<String, CellStyle>();
        CellStyle style;
        Font titleFont = wb.createFont();
        titleFont.setFontHeightInPoints((short)18);
        titleFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
        style = wb.createCellStyle();
        style.setAlignment(CellStyle.ALIGN_CENTER);
        style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
        style.setFont(titleFont);
        style.setFillForegroundColor(HSSFColor.BRIGHT_GREEN.index);
        style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
        styles.put("title", style);
        
        Font eventSourceFont = wb.createFont();
        eventSourceFont.setFontHeightInPoints((short)14);
        eventSourceFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
        style = wb.createCellStyle();
        style.setAlignment(CellStyle.ALIGN_CENTER);
        style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
        style.setFont(eventSourceFont);
        styles.put("eventSourceFont", style);
         
        Font itemFont = wb.createFont();
        itemFont.setFontHeightInPoints((short)14);
        itemFont.setFontName("Trebuchet MS");
        style = wb.createCellStyle();
        style.setAlignment(CellStyle.ALIGN_CENTER);
        style.setFont(itemFont);
        styles.put("rules", style);
        return styles;
	}
}
