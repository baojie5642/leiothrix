package xin.bluesky.leiothrix.server.bean.node;

/**
 * @author 张轲
 */
public class NodeInfo {

    private NodePhysicalInfo physicalInfo;

    private int workerNumbers;

    public NodeInfo() {
    }

    public NodeInfo(NodePhysicalInfo physicalInfo, int workerNumbers) {
        this.physicalInfo = physicalInfo;
        this.workerNumbers = workerNumbers;
    }

    public NodePhysicalInfo getPhysicalInfo() {
        return physicalInfo;
    }

    public void setPhysicalInfo(NodePhysicalInfo physicalInfo) {
        this.physicalInfo = physicalInfo;
    }

    public int getWorkerNumbers() {
        return workerNumbers;
    }

    public void setWorkerNumbers(int workerNumbers) {
        this.workerNumbers = workerNumbers;
    }
}
