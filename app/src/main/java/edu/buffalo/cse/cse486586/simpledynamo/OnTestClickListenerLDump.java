package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

/**
 * Created by zhenggangxue on 5/6/17.
 */

public class OnTestClickListenerLDump implements View.OnClickListener {
    private static final String TAG = OnTestClickListenerLDump.class.getName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;

    public OnTestClickListenerLDump(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {

            boolean gonnaQuery = true;


            if(gonnaQuery){
                publishProgress("Begin to query!\n");
                Cursor myCursor = testQuery();
                myCursor.moveToFirst();
                while (!myCursor.isAfterLast()){
                    publishProgress(myCursor.getString(0)+" "+myCursor.getString(1)+"\n");
                    Log.e(TAG, myCursor.getString(0)+" "+myCursor.getString(1));
                    myCursor.moveToNext();
                }
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);

            return;
        }

        private Cursor testQuery() {

            Cursor resultCursor = mContentResolver.query(mUri, null,
                    "@", null, null);
            Log.e(TAG, "Now in testQuery()");
            Log.e(TAG, "[testQuery()] finish query and got resultCursor");
            return resultCursor;
        }
    }

}
