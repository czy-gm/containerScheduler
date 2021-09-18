package com.tianchi.django.calculate;

import java.util.*;

import com.google.common.collect.*;
import com.tianchi.django.common.pojo.associate.PodPreAlloc;
import com.tianchi.django.common.utils.ScheduleUtils;
import com.tianchi.django.common.IReschedule;
import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.utils.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import static com.tianchi.django.calculate.AllocationStrategy.*;
import static com.tianchi.django.common.utils.HCollectors.countingInteger;
import static com.tianchi.django.common.utils.HCollectors.entriesToMap;
import static com.tianchi.django.common.utils.HCollectors.pairToMap;
import static java.util.stream.Collectors.*;

@Slf4j
@AllArgsConstructor
public class CalculateReschedule implements IReschedule {

    private long start;//程序启动时间戳

    public static Allocation bestReschedule = new Allocation();

    @Override
    public List<RescheduleResult> reschedule(final List<NodeWithPod> nodeWithPods, final Rule rule) {

        List<NodeWithPod> nodeWithPods4CheckAgainst = nodeWithPods.parallelStream().map(NodeWithPod::copy).collect(toList());

        calcReschedule(nodeWithPods4CheckAgainst, rule);
        return bestReschedule.getRescheduleResults();
        //return process(nodeWithPods4CheckAgainst, groupRuleAssociates, rule);
    }

    private void calcReschedule(List<NodeWithPod> nwps, Rule rule) {

        List<Pod> pods = new ArrayList<>();

        nwps.forEach(nodeWithPod -> pods.addAll(nodeWithPod.getPods().stream().map(Pod::copy).collect(toList())));

        List<NodeAndPod> naps = nwps.stream()
                .map(
                        nwp ->
                                NodeAndPod.builder()
                                        .node(nwp.getNode().copy())
                                        .pods(Lists.newArrayList())
                                        .surplusGpu(nwp.getNode().getGpu())
                                        .surplusCpu(nwp.getNode().getCpu())
                                        .surplusRam(nwp.getNode().getRam())
                                        .surplusDisk(nwp.getNode().getDisk())
                                        .surplusResource(nwp.getNode().getTopologies().stream().map(Topology::copy).collect(toList()))
                                        .podNumOfGroupMap(new HashMap<>())
                                        .podNum(0)
                                        .build()
                ).collect(toList());

        Map<String, List<Pod>> classifyPods = pods.stream().collect(groupingBy(Pod::getGroup));
        pods.clear();
        for (List<Pod> classifyPod : classifyPods.values()) {

            pods.addAll(classifyPod);
        }
        Collections.shuffle(pods);
        Comparator<Pod> first = Comparator.comparingDouble(pod -> (double)pod.getCpu() / pod.getRam());
        pods.sort(first);

        AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.set(RuleUtils.toAllMaxInstancePerNodeLimit(rule, GroupRuleAssociate.fromPods(pods)));

        simulateAnneal(nwps, naps, pods, rule);
    }


    private void simulateAnneal(List<NodeWithPod> nodeWithPodList, List<NodeAndPod> nodeAndPodList, List<Pod> podList, Rule rule) {

        //init
        List<NodeAndPod> newNaps = nodeAndPodList.stream().map(NodeAndPod::copy).collect(toList());

        List<Pod> pods = podList.stream().map(Pod::copy).collect(toList());

        List<NodeWithPod> newNwps = nodeWithPodList.stream().map(NodeWithPod::copy).collect(toList());

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(pods);

        //allocation
        allocation(newNaps, pods, rule);
        int alloScore = ScoreUtils.scoreNodeWithPods(newNaps.stream().map(NodeAndPod::incompleteCopy).collect(toList()), rule, groupRuleAssociates);

        //calc migrate
        List<RescheduleResult> rescheduleResults = calcMigrate(newNwps, newNaps, rule);
        int totalScore = alloScore + rescheduleResults.size();
        bestReschedule.setAndCopy(newNaps, pods, totalScore, rescheduleResults);

        log.info("initAllocation score is {}", totalScore);

        double temperature = INIT_TEMPERATURE;

        Allocation curAllcation = new Allocation();
        curAllcation.setAndCopy(newNaps, pods, alloScore + rescheduleResults.size());

        //pods = podList.stream().map(Pod::copy).collect(toList());
        while(temperature > TERMINAL_TEMPERATURE) {

            for (int i = 0; i < LOOP; i++) {

                newNaps = nodeAndPodList.stream().map(NodeAndPod::copy).collect(toList());
                newNwps = nodeWithPodList.stream().map(NodeWithPod::copy).collect(toList());
                pods = podList.stream().map(Pod::copy).collect(toList());
                newAllocationForReschedule(newNaps, pods, rule);

                alloScore = ScoreUtils.scoreNodeWithPods(newNaps.parallelStream().map(NodeAndPod::incompleteCopy).collect(toList()), rule, groupRuleAssociates);
                if (alloScore == -1) {
                    continue;
                }
                rescheduleResults = calcMigrate(newNwps, newNaps, rule);
                totalScore = alloScore + rescheduleResults.size();
                log.info("bestReschedule score is {}", bestReschedule.getScore());

                int scoreDiff = totalScore - curAllcation.getScore();

                if (scoreDiff < 0) {

                    curAllcation.setAndCopy(newNaps, pods, totalScore);

                    if (totalScore < bestReschedule.getScore()) {

                        bestReschedule.setAndCopy(newNaps, pods, totalScore, rescheduleResults);
                    }
                }
                else {

                    double random = Math.random();
                    if (Math.exp(-scoreDiff / temperature) > random) {

                        curAllcation.setAndCopy(newNaps, pods, totalScore);
                    }
                    else {

                        pods = curAllcation.getPods().parallelStream().map(Pod::copy).collect(toList());
                    }
                }
            }

            temperature *= DELTA;
        }


    }

    private List<RescheduleResult> calcMigrate(List<NodeWithPod> initNwps, List<NodeAndPod> reAlloNaps, Rule rule) {

        List<RescheduleResult> rescheduleResults = new ArrayList<>();

        List<MigratePod> migratePods = collectMigrate(initNwps, reAlloNaps);

        int stage = 1;

        List<NodeWithPod> releaseNodes = new ArrayList<>();

        for (NodeAndPod reAlloNap : reAlloNaps) {

            if (reAlloNap.getPodNum() == 0) {

                for (NodeWithPod initNwp : initNwps) {

                    if (reAlloNap.getNode().getSn().equals(initNwp.getNode().getSn())) {

                        releaseNodes.add(initNwp);
                        break;
                    }
                }
            }
        }

        while (CollectionUtils.isNotEmpty(migratePods)) {

            List<MigratePod> hasMigratePods = new ArrayList<>();

            for (MigratePod migratePod : migratePods) {

                boolean isMigrate = isMigrate(migratePod);
                if (isMigrate) {
                    hasMigratePods.add(migratePod);

                    rescheduleResults.add(RescheduleResult.builder().stage(stage++).podSn(migratePod.getInitPod().getPodSn())
                            .cpuIDs(migratePod.getReAlloPod().getCpuIDs()).sourceSn(migratePod.getSourceNwp().getNode().getSn())
                            .targetSn(migratePod.getTargetNwp().getNode().getSn()).build());
                }
            }

            if (CollectionUtils.isEmpty(hasMigratePods)) {

                if (!migrateIntrm(migratePods, releaseNodes, rescheduleResults, stage, rule)) {
                    log.error("handle deadlock failed.");
                    break;
                }
                stage++;
            }
            migratePods.removeAll(hasMigratePods);
        }

        return rescheduleResults;
    }

    private boolean migrateIntrm(List<MigratePod> migratePods, List<NodeWithPod> releaseNwps, List<RescheduleResult> rescheduleResults, int stage, Rule rule) {

        for (MigratePod migratePod : migratePods) {

            Map<String, Integer> allMaxInstancePerNodeLimit = AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();

            if (!migratePod.isIntrm()) {

                Utils.sortNode(releaseNwps, migratePod.getInitPod(), rule);
                for (NodeWithPod nwp : releaseNwps) {

                    if (!nwp.getNode().getSn().equals(migratePod.getSourceNwp().getNode().getSn())
                            && ScheduleUtils.resourceFillOnePod(nwp, migratePod.getInitPod())
                            && ScheduleUtils.layoutFillOnePod(allMaxInstancePerNodeLimit, nwp, migratePod.getInitPod())) {

                        migratePod.getSourceNwp().getPods().remove(migratePod.getInitPod());
                        nwp.getPods().add(migratePod.getInitPod());
                        migratePod.setIntrm(true);

                        rescheduleResults.add(RescheduleResult.builder().stage(stage)
                                .sourceSn(migratePod.getSourceNwp().getNode().getSn())
                                .targetSn(nwp.getNode().getSn())
                                .podSn(migratePod.getInitPod().getPodSn())
                                .cpuIDs(migratePod.getReAlloPod().getCpuIDs()).build());

                        migratePod.setSourceNwp(nwp);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean isMigrate(MigratePod migratePod) {

        Map<String, Integer> allMaxInstancePerNodeLimit = AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();

        NodeWithPod sourceNwp = migratePod.getSourceNwp();
        NodeWithPod targetNwp = migratePod.getTargetNwp();
        Pod initPod = migratePod.getInitPod();
        Pod reAlloPod = migratePod.getReAlloPod();

        if (!ScheduleUtils.resourceFillOnePod(targetNwp, reAlloPod)) {
            return false;
        }

        if (!ScheduleUtils.layoutFillOnePod(allMaxInstancePerNodeLimit, targetNwp, initPod)) {
            return false;
        }

        PodPreAlloc podPreAlloc = Utils.cgroupFillOnePod(targetNwp, initPod);
        if (!podPreAlloc.isSatisfy()) {
            return false;
        }

        if (CollectionUtils.isNotEmpty(podPreAlloc.getCpus())) {
            reAlloPod.setCpuIDs(podPreAlloc.getCpus());
        }

        targetNwp.getPods().add(reAlloPod);
        sourceNwp.getPods().remove(initPod);
        return true;
    }

    private List<MigratePod> collectMigrate(List<NodeWithPod> initNwps, List<NodeAndPod> reAlloNaps) {

        List<MigratePod> migratePods = new ArrayList<>();

        for (NodeWithPod initNwp : initNwps) {

            for (Pod initPod : initNwp.getPods()) {

                Pair<NodeAndPod, Pod> reAlloPos = isSamePosition(initNwp, reAlloNaps, initPod);

                if (reAlloPos != null) {
                    exchangePosition(initNwp, initPod, reAlloNaps, reAlloPos.getValue(), reAlloPos.getKey());
                }
            }
        }

        for (NodeWithPod initNwp : initNwps) {

            for (Pod initPod : initNwp.getPods()) {

                Pair<NodeAndPod, Pod> reAlloPos = isSamePosition(initNwp, reAlloNaps, initPod);

                if (reAlloPos != null) {
                    NodeWithPod targetNwp = findNodeWithPodBySn(initNwps, reAlloPos.getKey().getNode().getSn());
                    migratePods.add(MigratePod.builder().sourceNwp(initNwp).targetNwp(targetNwp).initPod(initPod)
                            .reAlloPod(reAlloPos.getValue()).intrm(false).sameNode(false).build());
                }
            }
        }
        return migratePods;
    }

    private Pair<NodeAndPod, Pod> isSamePosition(NodeWithPod initNwp, List<NodeAndPod> reAlloNaps, Pod initPod) {

        for (NodeAndPod reAlloNap : reAlloNaps) {

            for (Pod reAlloPod : reAlloNap.getPods()) {

                if (reAlloPod.getPodSn().equals(initPod.getPodSn())) {

                    if(!reAlloNap.getNode().getSn().equals(initNwp.getNode().getSn())) {

                        return ImmutablePair.of(reAlloNap, reAlloPod);
                    }
                    return null;
                }
            }
        }
        return null;
    }

    private void exchangePosition(NodeWithPod initNwp, Pod initPod, List<NodeAndPod> reAlloNaps, Pod reAlloPod, NodeAndPod oldNap) {

        Map<String, Integer> allMaxInstancePerNodeLimit = AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();

        if (!Utils.isValidOfCgroup(initNwp, initPod)) {
             return;
        }

        for (NodeAndPod reAlloNap : reAlloNaps) {

            if (initNwp.getNode().getSn().equals(reAlloNap.getNode().getSn())) {

                for (Pod pod : reAlloNap.getPods()) {

                    if (pod.getCpu() == reAlloPod.getCpu() && pod.getRam() == reAlloPod.getRam() && pod.getDisk() == reAlloPod.getDisk()
                            && initNwp.getPods().stream().noneMatch($ -> $.getPodSn().equals(pod.getPodSn()))) {

                        if (!pod.getGroup().equals(reAlloPod.getGroup())) {
                            int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(reAlloPod.getGroup());
                            if (reAlloNap.getPods().stream().filter($ -> $.getGroup().equals(reAlloPod.getGroup())).count() >= maxInstancePerNodeLimit) {
                                continue;
                            }
                            maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(pod.getGroup());
                            if (oldNap.getPods().stream().filter($ -> $.getGroup().equals(pod.getGroup())).count() >= maxInstancePerNodeLimit) {
                                continue;
                            }
                        }

                        String tmp = pod.getPodSn();
                        pod.setPodSn(reAlloPod.getPodSn());
                        reAlloPod.setPodSn(tmp);

                        if (!pod.getGroup().equals(reAlloPod.getGroup())) {

                            reAlloNap.minusPodNumOfGroup(pod.getGroup());
                            reAlloNap.addPodNumOfGroup(reAlloPod.getGroup());

                            oldNap.minusPodNumOfGroup(reAlloPod.getGroup());
                            oldNap.addPodNumOfGroup(pod.getGroup());

                            tmp = pod.getGroup();
                            pod.setGroup(reAlloPod.getGroup());
                            reAlloPod.setGroup(tmp);
                        }

                        tmp = pod.getAppName();
                        pod.setAppName(reAlloPod.getAppName());
                        reAlloPod.setAppName(tmp);

                    }
                }
            }

        }

    }

    private List<PendingMigratePod> collectPendingMigrate(List<NodeAndPod> initNaps, List<NodeAndPod> reAlloNaps) {

        List<PendingMigratePod> pendingMigratePods = new ArrayList<>();

        for (NodeAndPod initNap : initNaps) {

            List<Pod> initPods = initNap.getPods();

            for (Pod initPod : initPods) {

                Pair<NodeAndPod, Pod> find = isSameNap(reAlloNaps, initNap, initPod);
                if (find == null) {
                    continue;
                }
                if (find.getKey() == null) {
                    pendingMigratePods.add(PendingMigratePod.builder().sourceNap(initNap).targetNap(initNap).
                            initPod(initPod).reAlloPod(find.getValue()).sameNode(true).build());
                }
                else {
                    NodeAndPod targetNap = findNodeAndPodBySn(initNaps, find.getKey().getNode().getSn());
                    pendingMigratePods.add(PendingMigratePod.builder().sourceNap(initNap).targetNap(targetNap).
                            initPod(initPod).reAlloPod(find.getValue()).sameNode(false).build());
                }
            }

        }

        return pendingMigratePods;
    }

    private Pair<NodeAndPod, Pod> isSameNap(List<NodeAndPod> reAlloNaps, NodeAndPod initNap, Pod initPod) {

        for (NodeAndPod reAlloNap : reAlloNaps) {

            for (Pod reAlloPod : reAlloNap.getPods()) {

                if (reAlloPod.getPodSn().equals(initPod.getPodSn())) {

                    if (!reAlloNap.getNode().getSn().equals(initNap.getNode().getSn())) {
                        return ImmutablePair.of(reAlloNap, reAlloPod);
                    }
                    else if (!reAlloPod.getCpuIDs().containsAll(initPod.getCpuIDs())){
                        return ImmutablePair.of(null, reAlloPod);
                    }
                    else {
                        return null;
                    }
                }
            }

        }
        return null;
    }

    private static NodeAndPod findNodeAndPodBySn(List<NodeAndPod> nodeAndPods, String sn) {

        for (NodeAndPod nodeAndPod : nodeAndPods) {
            if (nodeAndPod.getNode().getSn().equals(sn)) {
                return nodeAndPod;
            }
        }
        return null;
    }

    private static NodeWithPod findNodeWithPodBySn(List<NodeWithPod> nodeWithPods, String sn) {

        for (NodeWithPod nodeWithPod : nodeWithPods) {
            if (nodeWithPod.getNode().getSn().equals(sn)) {
                return nodeWithPod;
            }
        }
        return null;
    }


    /*private static boolean migrate(PendingMigratePod pmp) {

        Map<String, Integer> allMaxInstancePerNodeLimit = AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();
        int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(pmp.getInitPod().getGroup());

        NodeAndPod sourceNap = pmp.getSourceNap();
        NodeAndPod targetNap = pmp.getTargetNap();
        Pod initPod = pmp.getInitPod();
        Pod reAlloPod = pmp.getReAlloPod();

        if (!Utils.resourceFillOnePod(targetNap, reAlloPod)) {
            return false;
        }

        if (!Utils.layoutFillOnePod(maxInstancePerNodeLimit, targetNap, reAlloPod)) {
            return false;
        }
        //sourceNap.freeCpu(initPod.getCpuIDs());
        sourceNap.plusResource(initPod);
        sourceNap.setPodNum(sourceNap.getPodNum() - 1);
        sourceNap.minusPodNumOfGroup(initPod.getGroup());
        //targetNap.useCpu(reAlloPod.getCpuIDs());
        targetNap.minusResource(initPod);
        targetNap.addPodNumOfGroup(initPod.getGroup());
        targetNap.addPodNum();
        return true;

    }*/

    /*private static boolean handleDeadlock(List<PendingMigratePod> pmps, List<NodeAndPod> releaseNode, List<RescheduleResult> rescheduleResults, int stage) {

        for (PendingMigratePod pmp : pmps) {

            Map<String, Integer> allMaxInstancePerNodeLimit = AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();
            int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(pmp.getInitPod().getGroup());

            if (!pmp.isIntrm()) {

                for (NodeAndPod intrmNap : releaseNode) {

                    if (Utils.resourceFillOnePod(intrmNap, pmp.getInitPod()) && Utils.layoutFillOnePod(maxInstancePerNodeLimit, intrmNap, pmp.getInitPod())) {
                        pmp.setIntrm(true);
                        NodeAndPod sourceNap = pmp.getSourceNap();
                        sourceNap.setPodNum(sourceNap.getPodNum() - 1);
                        sourceNap.plusResource(pmp.getInitPod());
                        sourceNap.minusPodNumOfGroup(pmp.getInitPod().getGroup());
                        intrmNap.addPodNum();
                        intrmNap.minusResource(pmp.getInitPod());
                        intrmNap.addPodNumOfGroup(pmp.getInitPod().getGroup());
                        pmp.setSourceNap(intrmNap);
                        rescheduleResults.add(RescheduleResult.builder().stage(stage).podSn(pmp.getInitPod().getPodSn())
                                .sourceSn(sourceNap.getNode().getSn()).targetSn(intrmNap.getNode().getSn())
                                .cpuIDs(pmp.getReAlloPod().getCpuIDs()).build());
                        return true;

                    }
                }

            }
        }
        return false;
    }

     */

    private static Pair<Pod, Boolean> isEqualAllocation(Pod before, List<Pod> after) {

        for (Pod afterPod : after) {

            if (afterPod.getPodSn().equals(before.getPodSn())) {
                if (before.getCpuIDs().containsAll(afterPod.getCpuIDs())) {
                    return ImmutablePair.of(afterPod, true);
                }
                else {
                    return ImmutablePair.of(afterPod, false);
                }
            }
        }
        return ImmutablePair.of(null, false);
    }

    /*private List<RescheduleResult> process(List<NodeWithPod> nwps, List<GroupRuleAssociate> groupRuleAssociates, Rule rule) {

        Map<String, List<Pod>> againstPods = searchAgainstPods(nwps, rule);

        log.info("reschedule againstPods: {}", againstPods.values().stream().mapToInt(List::size).sum());

        List<RescheduleResult> rescheduleResults = calculate(nwps, againstPods, rule);



        List<NodeAndPod> nodeAndPods = Utils.buildNodeAndPod(nwps);

        Comparator<AlloPod> match = Collections.reverseOrder(Comparator.comparingDouble(AlloPod::getMatchScore));

        List<AlloPod> alloPods = new ArrayList<>();

        for (NodeAndPod nap : nodeAndPods) {

            for(Pod pod : nap.getPods()) {

                alloPods.add(AlloPod.builder().alloNap(nap).pod(pod).matchScore(Utils.calcMatchScore(nap, pod, rule)).build());
            }
        }

        alloPods.sort(match);

        int stage = 2;
        for (int i = 0; i <= 1000; i++) {
            AlloPod alloPod = alloPods.get(i);
            String sourceSn = alloPod.getAlloNap().getNode().getSn();
            if (reallocation(nodeAndPods, alloPod, rule)) {
                rescheduleResults.add(
                        RescheduleResult.builder()
                                .podSn(alloPod.getPod().getPodSn())
                                .cpuIDs(alloPod.getPod().getCpuIDs())
                                .sourceSn(sourceSn)
                                .targetSn(alloPod.getAlloNap().getNode().getSn())
                                .stage(stage)
                                .build()
                );
                stage++;
                //int score = ScoreUtils.scoreNodeWithPods(nodeAndPods.parallelStream().map(NodeAndPod::incompleteCopy).collect(toList()), rule, groupRuleAssociates);
            }
        }

        return rescheduleResults;
    }

    private boolean reallocation(List<NodeAndPod> nodeAndPods, AlloPod alloPod, Rule rule) {

        Pod pod = alloPod.getPod();

        NodeAndPod alloNap = alloPod.getAlloNap();

        Map<String, Integer> allMaxInstancePerNodeLimit = ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();

        int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(pod.getGroup());

        Utils.sortNodes(nodeAndPods, pod, rule);

        List<Integer> cpuIds = pod.getCpuIDs();

        for (NodeAndPod nap : nodeAndPods) {
            if (nap.getNode().getSn().equals(alloNap.getNode().getSn()))
                continue;
            if (Utils.fillOnePod(nap, pod, maxInstancePerNodeLimit)) {
                alloNap.freeCpu(cpuIds);
                alloNap.plusResource(pod);
                alloNap.setPodNum(alloNap.getPodNum() - 1);
                alloNap.getPods().remove(pod);
                alloPod.setAlloNap(nap);
                //alloPod.setMatchScore(Utils.calcMatchScore(nap, pod, rule));
                return true;
            }
        }
        return false;
    }
*/

    /*private void dpReschedule(List<NodeWithPod> nodeWithPodList, List<NodeAndPod> nodeAndPodList, List<Pod> podList, Rule rule) {

        List<NodeAndPod> nodeAndPods = nodeAndPodList.stream().map(NodeAndPod::copy).collect(toList());

        List<Pod> pods = podList.stream().map(Pod::copy).collect(toList());

        dp(nodeAndPods, pods, rule);

    }

    private void dp(List<NodeAndPod> nodeAndPodList, List<Pod> podList, Rule rule) {

        Map<String, Integer> allMaxInstancePerNodeLimit = AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();

        List<Pod> pods = podList.stream().map(Pod::copy).collect(toList());

        int[][] allo = new int[1000][1000];
        String[][] alloIds = new String[1000][1000];

        log.info("pods is {}", pods.size());

            for (NodeAndPod nodeAndPod : nodeAndPodList) {
                if (CollectionUtils.isEmpty(pods)) {
                    break;
                }
                log.info("pods size: {}", pods.size());
                int nodeCpu = nodeAndPod.getNode().getCpu();
                int nodeRam = nodeAndPod.getNode().getRam();

                for(int[] tmp : allo) {
                    Arrays.fill(tmp, 0);
                }
                for (String[] tmp : alloIds) {
                    Arrays.fill(tmp, "");
                }

                for (int t = 0; t < pods.size(); t++) {

                    Pod pod = pods.get(t);
                    int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(pod.getGroup());
                    int podCpu = pod.getCpu();
                    int podRam = pod.getRam();
                    //log.info("index:{}", t);
                    for (int i = nodeCpu; i >= podCpu; i--) {

                        for (int j = nodeRam; j >= podRam; j--) {


                            if (StringUtils.isNotBlank(alloIds[i - podCpu][j - podRam])) {
                                List<Pod> selectPods = getPodsByIds(pods, alloIds[i - podCpu][j - podRam]);
                                if (!Utils.layoutFillOnePod(maxInstancePerNodeLimit, selectPods, pod)) {
                                    continue;
                                }


                                NodeAndPod tmpNap = nodeAndPod.copy();
                                selectPods.forEach(selectPod -> tmpNap.useCpu(selectPod.getCpuIDs()));
                                PodPreAlloc podPreAlloc = Utils.cgroupFillOnePod(tmpNap, pod);
                                if (!podPreAlloc.isSatisfy()) {
                                    continue;
                                }

                            }

                            if (allo[i][j] < allo[i - podCpu][j - podRam] + 2 * podCpu + podRam) {
                                alloIds[i][j] = alloIds[i - podCpu][j - podRam] == null ? "" : alloIds[i - podCpu][j - podRam].concat(Integer.toString(t).concat(","));
                            }
                            allo[i][j] = Math.max(allo[i][j], allo[i - podCpu][j - podRam] + 2 * podCpu + podRam);
                        }
                    }
                }

                List<Pod> selectPods = getPodsByIds(pods, alloIds[nodeCpu][nodeRam]);
                for (int i = 0; i < selectPods.size(); i++) {
                    Pod selectPod = selectPods.get(i);
                    int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(selectPod.getGroup());
                    if (!Utils.fillOnePod(nodeAndPod, selectPod, maxInstancePerNodeLimit)) {
                        log.error("allo pod failed. nodeSn:{}, podSn:{}", nodeAndPod.getNode().getSn(), selectPod.getPodSn());
                        nodeAndPod.setPods(Lists.newArrayList());
                        pods.addAll(selectPods.subList(0, i));
                        break;
                    }
                    pods.remove(selectPod);
                }
                if (CollectionUtils.isNotEmpty(nodeAndPod.getPods())) {
                    Utils.calcResourceUsedRatio(nodeAndPod);
                }
            }

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(pods);
        int score = ScoreUtils.scoreNodeWithPods(nodeAndPodList.stream().map(NodeAndPod::incompleteCopy).collect(toList()), rule, groupRuleAssociates);
        log.info("dp score is {}", score);
    }

    private List<Pod> getPodsByIds(List<Pod> pods, String ids) {

        List<Pod> selectPods = new ArrayList<>();
        String[] strIds = ids.split(",");
        for (String strId : strIds) {
            if (StringUtils.isNotBlank(strId)) {
                int id = Integer.parseInt(strId);
                selectPods.add(pods.get(id));
            }
        }
        return selectPods;
    }

     */

    private List<RescheduleResult> calculate(List<NodeWithPod> nodeWithPods, Map<String, List<Pod>> againstPods, Rule rule) {

        Set<String> sourceNodeSns = Sets.newHashSet(againstPods.keySet());

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(NodeWithPodUtils.toPods(nodeWithPods));

        Map<String, Integer> allMaxInstancePerNodeLimit = RuleUtils.toAllMaxInstancePerNodeLimit(rule, groupRuleAssociates);

        List<RescheduleResult> rescheduleResults = Lists.newArrayList();

        List<Pod> forsakePods = Lists.newArrayList();//装不下的容器

        for (Map.Entry<String, List<Pod>> entry : againstPods.entrySet()) {

            String sourceSn = entry.getKey();

            //以打散为目的对排序后的pod、node贪心循环
            for (Pod pod : entry.getValue()) {

                boolean forsake = true;//遗弃标识

                for (NodeWithPod nwp : nodeWithPods) {

                    if (ScheduleUtils.ruleOverrunTimeLimit(rule, start)) {//时间上限约束，超时跳出。
                        log.info("overrun time limit");
                        return rescheduleResults;
                    }

                    if (sourceSn.equals(nwp.getNode().getSn())) {
                        continue;
                    }

                    if (sourceNodeSns.contains(nwp.getNode().getSn())) {//有迁移动作的宿主机不在装容器。(明显不优)
                        continue;
                    }

                    if (ScheduleUtils.staticFillOnePod(nwp, pod, allMaxInstancePerNodeLimit)) {
                        forsake = false;
                        rescheduleResults.add(//动态迁移每一个stage是并行执行，每一次执行必须满足调度约束。
                            RescheduleResult.builder()
                                .stage(1).sourceSn(sourceSn).targetSn(nwp.getNode().getSn())
                                .podSn(pod.getPodSn()).cpuIDs(pod.getCpuIDs())
                                .build()
                        );
                        break;
                    }
                }

                if (forsake) {//所有的机器都不满足此容器分配
                    forsakePods.add(pod);
                }
            }
        }

        if (forsakePods.size() > 0) {
            log.error("forsake pod count: {}", forsakePods.size());
        }

        return rescheduleResults;

    }

    /**
     * 找出这个集群中违背规则的所有的容器列表(贪心)。
     */
    private Map<String, List<Pod>> searchAgainstPods(List<NodeWithPod> nodeWithPods, Rule rule) {

        Map<String, List<Pod>> result = Maps.newHashMap();

        //先过滤不满足资源分配的容器，nodeWithPods数据会被修改
        searchResourceAgainstPods(nodeWithPods)
            .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        //再过滤不满足布局的容器，nodeWithPods数据会被修改
        searchLayoutAgainstPods(nodeWithPods, rule)
            .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        //再过滤不满足cpu分配的容器，nodeWithPods数据会被修改
        searchCgroupAgainstPods(nodeWithPods)
            .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        return result;

    }

    //违背资源规则容器准备重新调度。<node_sn,List<Pod>>
    private Map<String, List<Pod>> searchResourceAgainstPods(List<NodeWithPod> nodeWithPods) {
        return nodeWithPods.parallelStream()
            .map(
                nwp -> {

                    List<Pod> againstPods = Lists.newArrayList(), tmpPods = Lists.newArrayList(), normalPods = Lists.newArrayList();

                    //校验资源不满足的容器
                    for (Pod pod : nwp.getPods()) {

                        boolean against = false;

                        for (Resource resource : Resource.class.getEnumConstants()) {

                            int nodeResource = nwp.getNode().value(resource);

                            int podsResource = PodUtils.totalResource(ListUtils.union(tmpPods, ImmutableList.of(pod)), resource);

                            if (nodeResource < podsResource) {
                                againstPods.add(pod);
                                against = true;
                                break;
                            }

                        }

                        if (!against) {
                            tmpPods.add(pod);
                        }

                    }

                    int eniAgainstPodSize = tmpPods.size() - nwp.getNode().getEni();

                    //校验超过eni约束的容器
                    if (eniAgainstPodSize > 0) {
                        againstPods.addAll(tmpPods.subList(0, eniAgainstPodSize));
                        normalPods.addAll(tmpPods.subList(eniAgainstPodSize, tmpPods.size() - 1));
                    } else {
                        normalPods.addAll(tmpPods);
                    }

                    nwp.setPods(normalPods);//贪心判断的正常容器继续放在该机器中

                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);

                }
            )
            .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());

    }

    //违背布局规则容器准备重新调度。<node_sn,List<Pod>>
    private Map<String, List<Pod>> searchLayoutAgainstPods(List<NodeWithPod> nodeWithPods, Rule rule) {

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(NodeWithPodUtils.toPods(nodeWithPods));

        Map<String, Integer> maxInstancePerNodes = RuleUtils.toAllMaxInstancePerNodeLimit(rule, groupRuleAssociates);

        return nodeWithPods.parallelStream()
            .map(
                nwp -> {

                    Map<String, Integer> groupCountPreNodeMap = Maps.newHashMap();

                    List<Pod> againstPods = Lists.newArrayList(), normalPods = Lists.newArrayList();

                    for (Pod pod : nwp.getPods()) {

                        int maxInstancePerNode = maxInstancePerNodes.get(pod.getGroup());

                        int oldValue = groupCountPreNodeMap.getOrDefault(pod.getGroup(), 0);

                        if (oldValue == maxInstancePerNode) {
                            againstPods.add(pod);
                            continue;
                        }

                        groupCountPreNodeMap.put(pod.getGroup(), oldValue + 1);

                        normalPods.add(pod);

                    }

                    nwp.setPods(normalPods);//贪心判断的正常容器继续放在该机器中

                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);

                }
            )
            .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());
    }

    //违背cpu绑核分配规则容器准备重新调度。<node_sn,List<Pod>>
    private Map<String, List<Pod>> searchCgroupAgainstPods(List<NodeWithPod> nodeWithPods) {
        return nodeWithPods.parallelStream()
            .map(
                nwp -> {

                    Node node = nwp.getNode();

                    //node中不存在topologies,不校验绑核。
                    if (CollectionUtils.isEmpty(node.getTopologies())) {
                        return ImmutablePair.<String, List<Pod>>nullPair();
                    }

                    //node中不存在topologies,不校验绑核。
                    if (CollectionUtils.isEmpty(nwp.getPods())) {
                        return ImmutablePair.<String, List<Pod>>nullPair();
                    }

                    List<Pod> againstPods = Lists.newArrayList();

                    //这台机器上重叠的cpuId分配
                    Set<Integer> againstCpuIds = NodeWithPodUtils.cpuIDCountMap(nwp).entrySet().stream()
                        .filter(entry -> entry.getValue() > 1).map(Map.Entry::getKey).collect(toSet());

                    Map<Integer, Integer> cpuToSocket = NodeUtils.cpuToSocket(node);

                    Map<Integer, Integer> cpuToCore = NodeUtils.cpuToCore(node);

                    List<Pod> normalPods = Lists.newArrayList();

                    for (Pod pod : nwp.getPods()) {

                        if (CollectionUtils.isEmpty(pod.getCpuIDs())) {//没有分配cpuId的容器
                            againstPods.add(pod);
                            continue;
                        }

                        //贪心选择包含重叠cpu的容器
                        Set<Integer> intersectionCpuIds = Sets.intersection(againstCpuIds, Sets.newHashSet(pod.getCpuIDs()));

                        if (CollectionUtils.isNotEmpty(intersectionCpuIds)) {
                            againstCpuIds.removeAll(pod.getCpuIDs());
                            againstPods.add(pod);
                            continue;
                        }

                        long socketCount = pod.getCpuIDs().stream().map(cpuToSocket::get).distinct().count();

                        if (socketCount > 1) {//跨socket容器
                            againstPods.add(pod);
                            continue;
                        }

                        Map<Integer, Integer> sameCoreMap = pod.getCpuIDs().stream().collect(countingInteger(cpuToCore::get))
                            .entrySet().stream().filter(entry -> entry.getValue() > 1).collect(entriesToMap());

                        if (MapUtils.isNotEmpty(sameCoreMap)) {
                            againstPods.add(pod);
                            continue;
                        }
                        //TODO 已经很复杂了，如果排名拉不开差距在增加sensitiveCpuBind数据校验

                        normalPods.add(pod);

                    }

                    nwp.setPods(normalPods);//贪心判断的正常容器继续放在该机器中

                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);

                }
            )
            .filter(pair -> !pair.equals(ImmutablePair.<String, List<Pod>>nullPair()))
            .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());
    }

}
