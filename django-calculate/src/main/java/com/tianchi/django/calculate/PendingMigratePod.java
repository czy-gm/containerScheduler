package com.tianchi.django.calculate;

import com.tianchi.django.common.pojo.Pod;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PendingMigratePod {

    private Pod initPod;

    private Pod reAlloPod;

    private NodeAndPod sourceNap;

    private NodeAndPod targetNap;

    private boolean intrm;

    private boolean sameNode = false;

    public PendingMigratePod copy() {
        return PendingMigratePod.builder().reAlloPod(reAlloPod.copy()).initPod(initPod.copy()).
                sourceNap(sourceNap.copy()).targetNap(targetNap.copy()).build();
    }
}
