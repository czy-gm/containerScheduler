package com.tianchi.django.calculate;


import com.tianchi.django.common.pojo.NodeWithPod;
import com.tianchi.django.common.pojo.Pod;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MigratePod {

    private Pod initPod;

    private Pod reAlloPod;

    private NodeWithPod sourceNwp;

    private NodeWithPod targetNwp;

    private boolean intrm;

    private boolean sameNode = false;

    public MigratePod copy() {
        return MigratePod.builder().reAlloPod(reAlloPod.copy()).initPod(initPod.copy()).
                sourceNwp(sourceNwp.copy()).targetNwp(targetNwp.copy()).build();
    }
}
