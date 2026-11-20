export const reconcileViewTemplate = `
<div class="card wide" v-show="tab===4">
  <h2>物化对账</h2>
  <p>
    窗内事件 {{recon.window_events}} / 金额 {{recon.window_amount}} ·
    run {{recon.run_id || '—'}} ·
    迟到桶 {{recon.late_bucket}} · 拒收 {{recon.rejects}} ·
    对齐 <b :class="recon.aligned?'ok':'late'">{{recon.aligned}}</b>
  </p>
  <table>
    <thead>
      <tr><th>窗口</th><th>状态</th><th>窗内笔数</th><th>台账笔数</th><th>分区键</th><th>分区对齐</th><th>迟到</th><th>整窗对齐</th></tr>
    </thead>
    <tbody>
      <tr v-for="r in reconAll" :key="r.window_id">
        <td>{{r.name}}</td>
        <td>{{r.status}}</td>
        <td>{{r.window_events}}</td>
        <td>{{r.mat_row_count}}</td>
        <td>
          <span v-for="p in r.partitions" :key="p.partition_key"
                style="margin-right:6px"
                :class="p.aligned ? 'ok' : 'late'">
            {{p.partition_key}}({{p.row_count}}/{{p.materialized_row_count}})
          </span>
          <span v-if="!r.partitions.length" class="muted">无窗内事件</span>
        </td>
        <td>
          <span v-if="r.partitions.length">{{ r.partitions.every(p=>p.aligned) ? '全对齐' : '有缺片' }}</span>
          <span v-else class="muted">—</span>
        </td>
        <td>{{r.late_bucket}}</td>
        <td :class="r.aligned?'ok':'late'">{{r.aligned}}</td>
      </tr>
    </tbody>
  </table>
  <p class="muted">分区读数与物化写路径同源：缺任一成功分片即判不对齐，半截合计无法装绿。</p>
</div>
`;
