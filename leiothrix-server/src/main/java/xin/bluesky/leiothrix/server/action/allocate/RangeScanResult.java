package xin.bluesky.leiothrix.server.action.allocate;

import java.util.ArrayList;
import java.util.List;

/**
 * 扫描某个表的range的结果.{@link #hasUnallocatedRange}和{@link #isTableFinished}这两的值是有一定排斥的,
 * 如前者为true,则后者不可能为true;如前者为false,后者可能为true,也可能为false.
 *
 * @author 张轲
 */
public class RangeScanResult {

    private List<String> unallocatedRangeNameList = new ArrayList();

    private boolean isTableFinished = false;

    public RangeScanResult() {
    }

    public List<String> getUnallocatedRangeNameList() {
        return unallocatedRangeNameList;
    }

    public void addUnallocatedRangeName(String rangeName) {
        this.unallocatedRangeNameList.add(rangeName);
    }

    public boolean isTableFinished() {
        return isTableFinished;
    }

    public void setIsTableFinished(boolean isTableFinished) {
        this.isTableFinished = isTableFinished;
    }

    public boolean hasUnallocatedRange() {
        return unallocatedRangeNameList.size() > 0;
    }

}
