package com.tianchi.django.calculate;

import java.util.*;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tianchi.django.common.ISchedule;
import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.utils.*;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static com.tianchi.django.calculate.AllocationStrategy.*;
import static com.tianchi.django.common.constants.DjangoConstants.SCORE_EMPTY_NODE_MODEL_NAME;
import static java.util.stream.Collectors.*;

@Slf4j
@AllArgsConstructor
public class CalculateSchedule implements ISchedule {

    private long start;//对象构建时传入的程序启动时间戳

    public static Allocation bestSchedule = new Allocation();


    @Override
    public List<ScheduleResult> schedule(@NonNull final List<Node> nodes, @NonNull final List<App> apps, @NonNull final Rule rule) {

        initResourceValue(rule);

        //TODO server sort
        List<NodeAndPod> nodeAndPods = sortAndInitNodeWithPods(nodes, rule);

        //TODO 应用分组的排序
        List<Pod> pods = sortAndInitPods(apps, nodeAndPods, rule);

        AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.set(RuleUtils.toAllMaxInstancePerNodeLimit(rule, GroupRuleAssociate.fromApps(apps)));

        simulateAnneal(nodeAndPods, pods, rule);

        return bestSchedule.getNodeAndPods().parallelStream()
            .map(
                nwp -> nwp.getPods().stream()
                    .map(
                        pod -> ScheduleResult.builder().sn(nwp.getNode().getSn()).group(pod.getGroup())
                            .cpuIDs(pod.getCpuIDs()).build()
                    )
                    .collect(toList())
            )
            .flatMap(Collection::stream).collect(toList());

    }

    /**
     * 通过node按照规则分数从大到小排序, 然后nodes信息转化为能够容乃下pod对象的NodeWithPod对象，与node对象一对一构建。
     *
     * @param nodes 宿主机列表
     * @param rule  规则对象
     * @return 能够容乃下pod对象的NodeWithPod对象
     */
    private List<NodeAndPod> sortAndInitNodeWithPods(List<Node> nodes, Rule rule) {
        Comparator<Node> first = Comparator.comparingDouble(node -> (double)node.getCpu() / node.getRam());
        return nodes.parallelStream()
            .sorted(first)
            .map(
                node -> NodeAndPod.builder().node(node).pods(Lists.newArrayList()).surplusGpu(node.getGpu()).
                        surplusCpu(node.getCpu()).surplusRam(node.getRam()).surplusDisk(node.getDisk()).
                        surplusResource(node.getTopologies()).build()
            )
            .collect(toList());
    }

    /**
     * 通过apps列表对象转化为实际要扩容的pod列表
     *
     * @param apps 应用列表
     * @return 通过app信息构建的pods列表
     */
    private List<Pod> sortAndInitPods(List<App> apps, List<NodeAndPod> naps, Rule rule) {

        List<Pod> pods = Lists.newArrayList();

        for (App app : apps) {
            pods.addAll(IntStream.range(0, app.getReplicas()).mapToObj($ -> PodUtils.toPod(app)).collect(toList()));//按照app排序构建pod数据
        }

        Collections.shuffle(pods);
        pods.sort(Collections.reverseOrder(Comparator.comparingDouble(pod -> (double)pod.getRam() / pod.getCpu())));

        log.info("schedule app transform pod count : {}", pods.size());

        return pods;
    }


    public static void simulateAnneal(List<NodeAndPod> nodeAndPodList, List<Pod> podList, Rule rule) {

        List<NodeAndPod> nodeAndPods = nodeAndPodList.stream().map(NodeAndPod::copy).collect(toList());
        List<Pod> pods = podList.stream().map(Pod::copy).collect(toList());

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(pods);

        AllocationStrategy.allocation(nodeAndPods, pods, rule);
        int score = ScoreUtils.scoreNodeWithPods(nodeAndPods.stream().map(NodeAndPod::incompleteCopy).collect(toList()), rule, groupRuleAssociates);

        bestSchedule.setAndCopy(nodeAndPods, pods, score);

        log.info("initAllocation score is {}", score);

        double temperature = INIT_TEMPERATURE;

        Allocation curAllcation = new Allocation();
        curAllcation.setAndCopy(nodeAndPods, pods, score);

        while(temperature > TERMINAL_TEMPERATURE) {

            for (int i = 0; i < LOOP; i++) {

                nodeAndPods = nodeAndPodList.parallelStream().map(NodeAndPod::copy).collect(toList());
                newAllocationForSchedule(nodeAndPods, pods, rule);

                score = ScoreUtils.scoreNodeWithPods(nodeAndPods.parallelStream().map(NodeAndPod::incompleteCopy).collect(toList()), rule, groupRuleAssociates);
                log.info("schedule score is {}", curAllcation.getScore());

                int scoreDiff = score - curAllcation.getScore();

                if (scoreDiff < 0) {

                    curAllcation.setAndCopy(nodeAndPods, pods, score);

                    if (score < bestSchedule.getScore()) {

                        bestSchedule.setAndCopy(nodeAndPods, pods, score);
                    }
                }
                else {

                    double random = Math.random();
                    if (Math.exp(-scoreDiff / temperature) > random) {

                        curAllcation.setAndCopy(nodeAndPods, pods, score);
                    }
                    else {

                        pods = curAllcation.getPods().parallelStream().map(Pod::copy).collect(toList());
                    }
                }
            }

            temperature *= DELTA;
        }


    }

    private void initResourceValue(Rule rule) {
        Map<Resource, Map<String, Integer>> scoreMap = RuleUtils.toResourceScoreMap(rule);

        Map<String, Integer> map = scoreMap.getOrDefault(Resource.GPU, Maps.newHashMap());
        Utils.gpuValue = map.getOrDefault(SCORE_EMPTY_NODE_MODEL_NAME, 0);

        map = scoreMap.getOrDefault(Resource.CPU, Maps.newHashMap());
        Utils.cpuValue = map.getOrDefault(SCORE_EMPTY_NODE_MODEL_NAME, 0);

        map = scoreMap.getOrDefault(Resource.RAM, Maps.newHashMap());
        Utils.ramValue = map.getOrDefault(SCORE_EMPTY_NODE_MODEL_NAME, 0);

        map = scoreMap.getOrDefault(Resource.DISK, Maps.newHashMap());
        Utils.diskValue = map.getOrDefault(SCORE_EMPTY_NODE_MODEL_NAME, 0);
        log.info("gpu:{}, cpu:{}, ram:{}, disk:{}", Utils.gpuValue, Utils.cpuValue, Utils.ramValue, Utils.diskValue);
    }


}
