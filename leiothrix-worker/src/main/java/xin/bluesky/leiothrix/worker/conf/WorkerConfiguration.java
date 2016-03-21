package xin.bluesky.leiothrix.worker.conf;

import xin.bluesky.leiothrix.worker.api.DatabasePageDataHandler;

/**
 * @author 张轲
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
