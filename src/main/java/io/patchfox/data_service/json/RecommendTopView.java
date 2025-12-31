package io.patchfox.data_service.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


@Setter
@Getter 
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Slf4j
public class RecommendTopView {

    public static final String REDUCE_CVES_TITLE = "REDUCE_CVES";
    public static final String REDUCE_CVES_ICON = "downhill_skiing";

    public static final String REDUCE_CVE_GROWTH_TITLE = "REDUCE_CVE_GROWTH";
    public static final String REDUCE_CVE_GROWTH_ICON = "trophy";

    public static final String REDUCE_CVE_BACKLOG_TITLE = "REDUCE_CVE_BACKLOG";
    public static final String REDUCE_CVE_BACKLOG_ICON = "self_improvement";

    public static final String REDUCE_CVE_BACKLOG_GROWTH_TITLE = "REDUCE_CVE_BACKLOG_GROWTH";
    public static final String REDUCE_CVE_BACKLOG_GROWTH_ICON = "social_leaderboard";

    public static final String REDUCE_STALE_PACKAGES_TITLE = "REDUCE_STALE_PACKAGES";
    public static final String REDUCE_STALE_PACKAGES_ICON = "snowboarding";

    public static final String REDUCE_STALE_PACKAGES_GROWTH_TITLE = "REDUCE_STALE_PACKAGES_GROWTH";
    public static final String REDUCE_STALE_PACKAGES_GROWTH_ICON = "rocket_launch";

    public static final String REDUCE_DOWNLEVEL_PACKAGES_TITLE = "REDUCE_DOWNLEVEL_PACKAGES";
    public static final String REDUCE_DOWNLEVEL_PACKAGES_ICON = "surfing";

    public static final String REDUCE_DOWNLEVEL_PACKAGES_GROWTH_TITLE = "REDUCE_DOWNLEVEL_PACKAGES_GROWTH";
    public static final String REDUCE_DOWNLEVEL_PACKAGES_GROWTH_ICON = "workspace_premium";

    public static final String GROW_PATCH_EFFICACY_TITLE = "GROW_PATCH_EFFICACY";
    public static final String GROW_PATCH_EFFICACY_ICON = "owl";

    public static final String REMOVE_REDUNDANT_PACKAGES_TITLE = "REMOVE_REDUNDANT_PACKAGES";
    public static final String REMOVE_REDUNDANT_PACKAGES_ICON = "raven";

    public enum Orientation { 
        LEFT, RIGHT
    }

    public enum Direction {
        UP, DOWN, LEFT, RIGHT
    }

    @JsonValue
    public List<TopCard> cards = new ArrayList<>();

    @AllArgsConstructor
    public class TopCard {
        @JsonProperty
        public String title;

        @JsonProperty
        public String headline;

        @JsonProperty
        public String iconName;

        @JsonProperty
        public Direction slideDirection; 

        @JsonProperty
        public Orientation iconOrientation;

        @JsonProperty
        public boolean useAlternateForegroundColorIfAvailable;
    }

    public void addCard(String title, String headline) { 
        var iconName = getIconForTitle(title);
        
        Random randy = new Random();
        
        var directionCeiling = Direction.values().length;
        var direction = Direction.values()[randy.nextInt(directionCeiling)];
        
        var orientationCeiling = Orientation.values().length;
        var orientation = Orientation.values()[randy.nextInt(orientationCeiling)];

        var useAlternateForegroundColorIfAvailable = randy.nextInt(2) == 0 ? false : true;

        var card = new TopCard(title, headline, iconName, direction, orientation, useAlternateForegroundColorIfAvailable);
        this.cards.add(card); 
    }

    public String getIconForTitle(String title) {
        switch (title) {
            case REDUCE_CVES_TITLE:
                return REDUCE_CVES_ICON;
            case REDUCE_CVE_GROWTH_TITLE:
                return REDUCE_CVE_GROWTH_ICON;  
            case REDUCE_CVE_BACKLOG_TITLE:
                return REDUCE_CVE_BACKLOG_ICON;              
            case REDUCE_CVE_BACKLOG_GROWTH_TITLE:
                return REDUCE_CVE_BACKLOG_GROWTH_ICON;
            case REDUCE_STALE_PACKAGES_TITLE:
                return REDUCE_STALE_PACKAGES_ICON;
            case REDUCE_STALE_PACKAGES_GROWTH_TITLE:
                return REDUCE_STALE_PACKAGES_GROWTH_ICON;
            case REDUCE_DOWNLEVEL_PACKAGES_TITLE:
                return REDUCE_DOWNLEVEL_PACKAGES_ICON;
            case REDUCE_DOWNLEVEL_PACKAGES_GROWTH_TITLE:
                return REDUCE_DOWNLEVEL_PACKAGES_GROWTH_ICON;
            case GROW_PATCH_EFFICACY_TITLE:
                return GROW_PATCH_EFFICACY_ICON;
            case REMOVE_REDUNDANT_PACKAGES_TITLE:
                return REMOVE_REDUNDANT_PACKAGES_ICON;
            default:
                return "";
        }
    }
}
