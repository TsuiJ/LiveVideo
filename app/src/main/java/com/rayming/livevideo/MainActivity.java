package com.rayming.livevideo;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.MotionEvent;

import com.utils.ijkplayer.common.PlayerManager;

public class MainActivity extends AppCompatActivity implements PlayerManager.PlayerStateListener{

    private PlayerManager player;

    private final String url = "http://120.76.248.89:8100/hls/captcha.m3u8";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_video);
        this.initPlayer();
    }

    private void initPlayer() {
        player = new PlayerManager(this);
        player.setFullScreenOnly(true);
        player.setScaleType(PlayerManager.SCALETYPE_FILLPARENT);
        player.playInFullScreen(true);
        player.setPlayerStateListener(this);
        player.play(url);
    }

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        if (player.gestureDetector.onTouchEvent(event))
            return true;
        return super.onTouchEvent(event);
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void onError() {

    }

    @Override
    public void onLoading() {

    }

    @Override
    public void onPlay() {

    }
}
