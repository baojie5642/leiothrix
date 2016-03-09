package xin.bluesky.leiothrix.worker;

import xin.bluesky.leiothrix.worker.api.DatabasePageDataHandler;

/**
 * @author 张轲
 * @date 16/2/15
 */
public class WorkerConfiguration {
    private DatabasePageDataHandler databasePageDataHandler;

    public DatabasePageDataHandler getDatabasePageDataHandler() {
        return databasePageDataHandler;
    }

    public void setDatabasePageDataHandler(DatabasePageDataHandler databasePageDataHandler) {
        this.databasePageDataHandler = databasePageDataHandler;
    }

}
