package io.sharedstreets.matcher.input.model;


import com.esri.core.geometry.Polyline;

import java.io.Serializable;


public class SharedStreetGeometry implements Serializable {


    public enum ROAD_CLASS {

        ClassMotorway(0),
        ClassTrunk(1),
        ClassPrimary(2),
        ClassSecondary(3),
        ClassTertiary(4),
        ClassResidential(5),
        ClassUnclassified(6),
        ClassService(7),
        ClassOther(8);

        private final int value;

        ROAD_CLASS(final int newValue) {
            value = newValue;
        }

        public static ROAD_CLASS forInt(int id) {

            for (ROAD_CLASS roadClass : values()) {
                if (roadClass.value == id) {
                    return roadClass;
                }
            }
            throw new IllegalArgumentException("Invalid roadClass id: " + id);

        }

        public int getValue() {
            return value;
        }

    }

    public String id;
    public Polyline geometry;
    public ROAD_CLASS roadClass;
    public String startIntersectionId;
    public String endIntersectionId;
    public String forwardReferenceId;
    public String backReferenceId;
    public Double speed;
    public Double accleration;
    public Double braking;
    public Double speeding;


    public SharedStreetGeometry() {

    }

    // speed in km/h
    public int getSpeed() {

        if(roadClass.equals(ROAD_CLASS.ClassMotorway))
            return 130;
        else if(roadClass.equals(ROAD_CLASS.ClassTrunk))
            return 120;
        else if(roadClass.equals(ROAD_CLASS.ClassPrimary))
            return 110;
        else if(roadClass.equals(ROAD_CLASS.ClassSecondary))
            return 80;
        else if(roadClass.equals(ROAD_CLASS.ClassTertiary))
            return 80;
        else if(roadClass.equals(ROAD_CLASS.ClassResidential))
            return 80;
        else if(roadClass.equals(ROAD_CLASS.ClassUnclassified))
            return 80;
        else
            return 80;
    }

    public float getPriority() {
        if(roadClass.equals(ROAD_CLASS.ClassMotorway))
            return 0.9f;
        else if(roadClass.equals(ROAD_CLASS.ClassTrunk))
            return 1.1f;
        else if(roadClass.equals(ROAD_CLASS.ClassPrimary))
            return 1.2f;
        else if(roadClass.equals(ROAD_CLASS.ClassSecondary))
            return 1.4f;
        else if(roadClass.equals(ROAD_CLASS.ClassTertiary))
            return 1.6f;
        else if(roadClass.equals(ROAD_CLASS.ClassResidential))
            return 2.5f;
        else if(roadClass.equals(ROAD_CLASS.ClassUnclassified))
            return 3.0f;
        else
            return 5.0f;
    }

}
