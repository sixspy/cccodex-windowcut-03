export const materializeViewTemplate = `
<div class="card wide" v-show="tab===3">
  <h2>物化台账</h2>
  <p class="muted">
    分区物化册（{{registry.policy_date || '当日未落册'}}）：
    <b :class="registry.path==='整窗回滚' ? 'ok' : 'late'">{{registry.path || '整窗回滚'}}</b>
    · 冻结 {{registry.frozen}} · 任一分片失败，本窗物化行与分区标记同事务回滚，台账零增。
  </p>
  <p class="muted">
    失败模拟（夹具）：窗口 ID
    <input v-model="matForm.wid" style="width:70px" />
    失败分区
    <input v-model="matForm.fail_partition" style="width:70px" placeholder="B" />
    <button class="ghost" @click="materializeFail(matForm.wid, matForm.fail_partition)">造半截失败</button>
    <button class="ghost" @click="loadRegistry">刷新物化册</button>
  </p>
  <table>
    <thead><tr><th>窗口</th><th>run</th><th>状态</th><th>行数</th><th>金额</th><th>分区键</th><th>时间</th></tr></thead>
    <tbody>
      <tr v-for="m in mats" :key="m.id">
        <td>{{m.window_name}}</td>
        <td>{{m.run_id}}</td>
        <td :class="m.status==='成功' ? 'ok' : 'late'">{{m.status}}</td>
        <td>{{m.row_count}}</td>
        <td>{{m.total_amount}}</td>
        <td>{{m.partition_key}}</td>
        <td>{{m.created_at}}</td>
      </tr>
    </tbody>
  </table>
  <p class="muted" v-if="!mats.length">台账为空：无成功物化行，也无失败凭证。</p>
</div>
`;
