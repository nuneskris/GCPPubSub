package com.nuneskris.study.gcp.pubsub;

public class CricketDelivery {

    private String id;
    private int inning;
    private int over;
    private int ball;
    private String batsman;
    private String non_striker;
    private String bowler;
    private int batsman_runs;
    private int extra_runs;
    private int total_runs;
    private int non_boundary;
    private int is_wicket;
    private String dismissal_kind;
    private String player_dismissed;
    private String fielder;
    private String extras_type;
    private String batting_team;
    private String bowling_team;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getInning() {
        return inning;
    }

    public void setInning(int inning) {
        this.inning = inning;
    }

    public int getOver() {
        return over;
    }

    public void setOver(int over) {
        this.over = over;
    }

    public int getBall() {
        return ball;
    }

    public void setBall(int ball) {
        this.ball = ball;
    }

    public String getBatsman() {
        return batsman;
    }

    public void setBatsman(String batsman) {
        this.batsman = batsman;
    }

    public String getNon_striker() {
        return non_striker;
    }

    public void setNon_striker(String non_striker) {
        this.non_striker = non_striker;
    }

    public String getBowler() {
        return bowler;
    }

    public void setBowler(String bowler) {
        this.bowler = bowler;
    }

    public int getBatsman_runs() {
        return batsman_runs;
    }

    public void setBatsman_runs(int batsman_runs) {
        this.batsman_runs = batsman_runs;
    }

    public int getExtra_runs() {
        return extra_runs;
    }

    public void setExtra_runs(int extra_runs) {
        this.extra_runs = extra_runs;
    }

    public int getTotal_runs() {
        return total_runs;
    }

    public void setTotal_runs(int total_runs) {
        this.total_runs = total_runs;
    }

    public int getNon_boundary() {
        return non_boundary;
    }

    public void setNon_boundary(int non_boundary) {
        this.non_boundary = non_boundary;
    }

    public int getIs_wicket() {
        return is_wicket;
    }

    public void setIs_wicket(int is_wicket) {
        this.is_wicket = is_wicket;
    }

    public String getDismissal_kind() {
        return dismissal_kind;
    }

    public void setDismissal_kind(String dismissal_kind) {
        this.dismissal_kind = dismissal_kind;
    }

    public String getPlayer_dismissed() {
        return player_dismissed;
    }

    public void setPlayer_dismissed(String player_dismissed) {
        this.player_dismissed = player_dismissed;
    }

    public String getFielder() {
        return fielder;
    }

    public void setFielder(String fielder) {
        this.fielder = fielder;
    }

    public String getExtras_type() {
        return extras_type;
    }

    public void setExtras_type(String extras_type) {
        this.extras_type = extras_type;
    }

    public String getBatting_team() {
        return batting_team;
    }

    public void setBatting_team(String batting_team) {
        this.batting_team = batting_team;
    }

    public String getBowling_team() {
        return bowling_team;
    }

    public void setBowling_team(String bowling_team) {
        this.bowling_team = bowling_team;
    }
}
