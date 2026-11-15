import React, { useCallback, useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, useLocation, useNavigate } from 'react-router-dom';
import { App as AntApp, Alert, Badge, Button, Card, Checkbox, ConfigProvider, Descriptions, Drawer, Empty, Form, Input, InputNumber,
  Layout, Menu, Modal, QRCode, Select, Space, Spin, Statistic, Switch, Table, Tag, Tooltip, Typography, theme as antTheme } from 'antd';
import ruRU from 'antd/locale/ru_RU'; import enUS from 'antd/locale/en_US';
import { ApartmentOutlined, ApiOutlined, DashboardOutlined, DatabaseOutlined, DeploymentUnitOutlined, FileTextOutlined,
  LineChartOutlined, LogoutOutlined, MoonOutlined, PlusOutlined, SafetyCertificateOutlined, SettingOutlined, SunOutlined, TeamOutlined } from '@ant-design/icons';
import type { Entity, Node, Observation, Operation, Point, Profile, Session, TokenInfo } from './types';
import { api, ApiError, operationId, session } from './api';
import { en, ru } from './i18n'; import type { Label } from './i18n';
import './style.css';

const { Text, Title, Paragraph } = Typography;
const number = (value?: number | string | null) => value == null ? '—' : new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(typeof value === 'string' ? BigInt(value) : value);
const bytes = (input?: number | string | null) => {
  const value = input == null ? null : Number(input);
  if (value == null) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']; const power = Math.min(4, Math.floor(Math.log(Math.max(1, value)) / Math.log(1024)));
  return number(value / 1024 ** power) + ' ' + units[power];
};
type T = (key: Label) => string;
const isFreshNode = (node: Node) => Date.now() - Date.parse(node.resources.observedAt) < Math.max(5, (node.resources.sampleIntervalSeconds ?? 1) * 2) * 1000;
type UserRow = { id: string; userName: string; role: string; clusterIds: string[]; twoFactorEnabled: boolean; canReadPayload: boolean };
type Preview = { messageId: string; payloadLength: number; base64: string; truncated: boolean };
const localDateTime = (date: Date) => new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16);

function Chart({ points, field, t }: { points: Point[]; field: 'messagesPerSecond' | 'p99Milliseconds'; t: T }) {
  const [hover, setHover] = useState<number>();
  const values = points.map(point => point[field]); const maximum = Math.max(1, ...values.filter((value): value is number => value != null));
  const x = (index: number) => 50 + index * 720 / Math.max(1, points.length - 1); const y = (value: number) => 180 - value * 150 / maximum;
  let drawing = ''; let gap = true;
  values.forEach((value, index) => { if (value == null) gap = true; else { drawing += (gap ? 'M' : 'L') + x(index) + ',' + y(value) + ' '; gap = false; } });
  const selected = hover == null ? points.at(-1) : points[hover];
  if (!points.length) return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={t('empty')} />;
  return <div className="chart" onMouseLeave={() => setHover(undefined)}>
    <div className="chart-value">{selected ? new Date(selected.at).toLocaleTimeString() : '—'} · <b>{number(selected?.[field])}</b> {field === 'p99Milliseconds' ? 'ms' : 'msg/s'}</div>
    <svg role="img" aria-label={t(field === 'p99Milliseconds' ? 'latency' : 'throughput')} viewBox="0 0 800 215"
      onMouseMove={event => { const rect = event.currentTarget.getBoundingClientRect(); setHover(Math.max(0, Math.min(points.length - 1, Math.round(((event.clientX - rect.left) / rect.width * 800 - 50) / 720 * (points.length - 1))))); }}>
      {[0, .5, 1].map(ratio => <g key={ratio}><line x1="50" x2="770" y1={y(maximum * ratio)} y2={y(maximum * ratio)} className="gridline" />
        <text x="40" y={y(maximum * ratio) + 4} textAnchor="end">{number(maximum * ratio)}</text></g>)}
      <path d={drawing} fill="none" stroke="var(--accent)" strokeWidth="2.5" />
      <text x="50" y="207">{new Date(points[0].at).toLocaleTimeString()}</text><text x="770" y="207" textAnchor="end">{new Date(points.at(-1)!.at).toLocaleTimeString()}</text>
      {hover != null && selected?.[field] != null && <circle cx={x(hover)} cy={y(selected[field]!)} r="5" fill="var(--accent)" />}
    </svg></div>;
}

function DashboardTheme() {
  const [language, setLanguage] = useState(localStorage.getItem('raft.language') ?? 'ru');
  const [dark, setDark] = useState(localStorage.getItem('raft.theme') !== 'light');
  return <ConfigProvider locale={language === 'ru' ? ruRU : enUS} theme={{ algorithm: dark ? antTheme.darkAlgorithm : antTheme.defaultAlgorithm,
    token: { colorPrimary: dark ? '#c3ed78' : '#467d24', colorBgLayout: dark ? '#0c1512' : '#f4f7f2', colorBgContainer: dark ? '#16211b' : '#ffffff',
      colorTextLightSolid: dark ? '#18240e' : '#ffffff', borderRadius: 12, fontFamily: '"Segoe UI", system-ui, sans-serif', colorText: dark ? '#e2e9dd' : '#18251e' } }}>
    <AntApp><Dashboard language={language} dark={dark} setLanguage={setLanguage} setDark={setDark} /></AntApp>
  </ConfigProvider>;
}
function Dashboard({ language, dark, setLanguage, setDark }: {
  language: string; dark: boolean; setLanguage: React.Dispatch<React.SetStateAction<string>>; setDark: React.Dispatch<React.SetStateAction<boolean>>;
}) {
  const labels = language === 'ru' ? ru : en; const t: T = key => labels[key];
  const [currentSession, setSession] = useState<Session>(); const [profiles, setProfiles] = useState<Profile[]>([]);
  const [authorizedUser, setAuthorizedUser] = useState<string>();
  const [observations, setObservations] = useState<Observation[]>([]); const [selected, setSelected] = useState(localStorage.getItem('raft.cluster') ?? '');
  const [streamConnected, setStreamConnected] = useState(false);
  const lastEventAt = useRef(0); const [, setClock] = useState(0);
  const [dialog, setDialog] = useState<string>(); const [inspected, setInspected] = useState<Node>();
  const [entity, setEntity] = useState<Entity>(); const [tokens, setTokens] = useState<TokenInfo[]>([]);
  const [audit, setAudit] = useState<Record<string, unknown>[]>([]); const [users, setUsers] = useState<UserRow[]>([]);
  const [auditSource, setAuditSource] = useState('cluster');
  const [editingUser, setEditingUser] = useState<UserRow>(); const [secret, setSecret] = useState<string>();
  const [editingToken, setEditingToken] = useState<TokenInfo>();
  const [preview, setPreview] = useState<Preview>();
  const [configuration, setConfiguration] = useState<Record<string, string>>({});
  const [historyRange, setHistoryRange] = useState('live'); const [minuteHistory, setMinuteHistory] = useState<Point[]>([]);
  const [dialogOperation, setDialogOperation] = useState(operationId);
  const [authenticator, setAuthenticator] = useState<string>(); const [recoveryCodes, setRecoveryCodes] = useState<string[]>();
  const [loading, setLoading] = useState(false); const [chartMetric, setChartMetric] = useState<'messagesPerSecond' | 'p99Milliseconds'>('messagesPerSecond');
  const { message, modal } = AntApp.useApp(); const navigate = useNavigate(); const location = useLocation();
  const section = location.pathname.split('/')[1] || 'overview'; const [form] = Form.useForm();
  const observation = observations.find(item => item.id === selected); const status = observation?.status;
  const ownsView = !!currentSession?.username && authorizedUser === currentSession.username;
  useEffect(() => {
    const offline = () => setStreamConnected(false);
    window.addEventListener('offline', offline);
    const timer = window.setInterval(() => {
      setClock(value => value + 1); // Re-evaluate stale timestamps even when a TCP stream stalls silently.
      if (!navigator.onLine || Date.now() - lastEventAt.current > Math.max(10, (observation?.staleAfterSeconds ?? 5) * 2) * 1000) setStreamConnected(false);
    }, 1000);
    return () => { window.clearInterval(timer); window.removeEventListener('offline', offline); };
  }, [observation?.staleAfterSeconds]);
  const canWrite = currentSession?.role === 'Admin' || currentSession?.role === 'Operator'; const admin = currentSession?.role === 'Admin';
  const needsEnrollment = currentSession?.mfaRequired && !currentSession.mfaEnabled;
  const activeOperation = observation?.operations.some(operation => operation.state === 'Accepted');
  const refreshSession = useCallback(async () => { const value = await session(); setSession(value); return value; }, []);
  const error = (reason: unknown) => {
    if (reason instanceof ApiError && reason.status === 401) { void refreshSession(); }
    void message.error(reason instanceof ApiError && reason.code === 'reauth' ? t('reauth') : t('failure') + ': ' + (reason instanceof Error ? reason.message : ''));
  };
  const refreshProfiles = useCallback(async () => {
    const values = await api<Profile[]>('admin/clusters'); setProfiles(values);
    setSelected(previous => values.some(profile => profile.id === previous) ? previous : values[0]?.id ?? '');
  }, []);
  useEffect(() => { void refreshSession(); }, [refreshSession]);
  useEffect(() => {
    setProfiles([]); setObservations([]); setTokens([]); setAudit([]); setUsers([]); setConfiguration({});
    setSecret(undefined); setPreview(undefined); setInspected(undefined); setDialog(undefined); setAuthenticator(undefined); setRecoveryCodes(undefined);
    setAuthorizedUser(currentSession?.username);
  }, [currentSession?.username]);
  useEffect(() => { document.documentElement.lang = language; document.documentElement.dataset.theme = dark ? 'dark' : 'light';
    localStorage.setItem('raft.language', language); localStorage.setItem('raft.theme', dark ? 'dark' : 'light'); }, [language, dark]);
  useEffect(() => { localStorage.setItem('raft.cluster', selected); }, [selected]);
  useEffect(() => { setTokens([]); setAudit([]); setConfiguration({}); setMinuteHistory([]); setDialog(undefined); setSecret(undefined); setPreview(undefined); setInspected(undefined); }, [selected]);
  useEffect(() => {
    if (!currentSession?.username || needsEnrollment) return;
    void refreshProfiles();
    const stream = new EventSource('/api/admin/stream');
    stream.addEventListener('observation', event => { setObservations(JSON.parse((event as MessageEvent).data)); lastEventAt.current = Date.now(); setStreamConnected(navigator.onLine); });
    stream.onerror = () => setStreamConnected(false);
    return () => stream.close();
  }, [currentSession?.username, needsEnrollment, refreshProfiles]);
  useEffect(() => {
    if (!currentSession?.username || needsEnrollment) return;
    let cancelled = false;
    if (section === 'tokens' && selected && admin) void api<TokenInfo[]>('admin/clusters/' + selected + '/tokens').then(value => { if (!cancelled) setTokens(value); }).catch(error);
    if (section === 'audit' && selected) void api<Record<string, unknown>[]>(auditSource === 'dashboard' && admin ? 'admin/dashboard-audit' : 'admin/clusters/' + selected + '/audit').then(value => { if (!cancelled) setAudit(value); }).catch(error);
    if (section === 'users' && admin) void api<UserRow[]>('admin/users').then(value => { if (!cancelled) setUsers(value); }).catch(error);
    if (section === 'settings' && selected) void api<Record<string, string>>('admin/clusters/' + selected + '/configuration').then(value => { if (!cancelled) setConfiguration(value); }).catch(error);
    if (section === 'overview' && selected && historyRange !== 'live') void api<Point[]>('admin/clusters/' + selected + '/history').then(value => { if (!cancelled) setMinuteHistory(value); }).catch(error);
    return () => { cancelled = true; };
  // Data is fetched on navigation; mutations explicitly refresh the affected table.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [section, selected, historyRange, auditSource, currentSession?.username, needsEnrollment]);
  function open(kind: string, values?: object) { form.resetFields(); if (values) form.setFieldsValue(values); setDialogOperation(operationId()); setDialog(kind); }
  async function run(action: () => Promise<unknown>, accepted = false, closeDialog = true) {
    setLoading(true);
    try { await action(); void message.success(t(accepted ? 'actionAccepted' : 'saved')); if (closeDialog) setDialog(undefined); }
    catch (reason) { error(reason); } finally { setLoading(false); }
  }
  function operation(kind: string, nodeId?: string) {
    modal.confirm({ title: t(kind === 'remove' ? 'remove' : kind === 'transfer' ? 'transfer' : 'snapshot'), content: t('dangerous'), okText: t('confirm'), cancelText: t('close'),
      onOk: () => run(() => api('admin/clusters/' + selected + '/operations', { operation: { kind, nodeId }, operationId: operationId() }), true) });
  }
  const last = observation?.history.at(-1); const freshNodes = observation?.nodes.filter(isFreshNode) ?? [];
  const httpFailures = freshNodes.length ? freshNodes.reduce((total, node) => total + BigInt(node.failedRequests), 0n).toString() : undefined;
  const stale = !!observation?.error || !observation?.observedAt || Date.now() - Date.parse(observation.observedAt) > (observation.staleAfterSeconds ?? 5) * 1000;
  const metrics = [
    { title: t('online'), value: freshNodes.length + ' / ' + ((status?.voters.length ?? 0) + (status?.learners.length ?? 0)), foot: t('quorum') + ' ' + (status ? Math.floor(status.voters.length / 2) + 1 : '—') },
    { title: t('rate'), value: number(last?.messagesPerSecond), foot: 'msg/s · ' + t('httpFailures') + ': ' + number(httpFailures) },
    { title: t('latency'), value: number(last?.p99Milliseconds), foot: 'ms', tooltip: t('metricHint') },
    { title: t('disk'), value: bytes(last?.physicalStorageBytes), foot: t('log') + ' + ' + t('history') + ' + ' + t('images') },
  ];
  const roleTag = (role: string) => <Tag color={role === 'Leader' ? 'lime' : role === 'Learner' ? 'blue' : 'default'}>{role}</Tag>;
  const nodeColumns = [
    { title: t('nodeId'), key: 'id', render: (_: unknown, node: Node) => <Button type="link" onClick={() => setInspected(node)}>{node.status.nodeId}</Button> },
    { title: t('role'), key: 'role', render: (_: unknown, node: Node) => roleTag(node.status.role) },
    { title: t('state'), key: 'state', render: (_: unknown, node: Node) => <Badge status={isFreshNode(node) ? 'success' : 'warning'} text={t(isFreshNode(node) ? 'live' : 'stale')} /> },
    { title: t('lag'), key: 'lag', render: (_: unknown, node: Node) => { const lag = status ? BigInt(status.lastLogIndex) - BigInt(node.status.lastLogIndex) : undefined; return number(lag == null ? undefined : String(lag > 0n ? lag : 0n)); } },
    { title: t('applied'), key: 'applied', render: (_: unknown, node: Node) => number(node.status.lastApplied) },
    { title: t('workingSet'), key: 'ram', render: (_: unknown, node: Node) => bytes(node.resources.workingSetBytes) },
    { title: '', key: 'actions', render: (_: unknown, node: Node) => canWrite && <Space>
      <Button size="small" disabled={activeOperation || node.status.role === 'Leader' || !status?.voters.includes(node.status.nodeId)} onClick={() => operation('transfer', node.status.nodeId)}>{t('transfer')}</Button>
      <Button size="small" danger disabled={activeOperation} onClick={() => operation('remove', node.status.nodeId)}>{t('remove')}</Button></Space> },
  ];
  const operationColumns = [
    { title: 'ID', dataIndex: 'operationId', render: (value: string) => <Text copyable className="mono">{value}</Text> },
    { title: t('name'), key: 'kind', render: (_: unknown, item: Operation) => item.request.kind + ' ' + (item.request.nodeId ?? item.request.add?.peer.nodeId ?? '') },
    { title: t('state'), key: 'state', render: (_: unknown, item: Operation) => <Tag color={item.state === 'Failed' ? 'red' : item.state === 'Accepted' ? 'gold' : 'green'}>{item.membershipStage ?? item.state}</Tag> },
    { title: t('source'), dataIndex: 'actor' }, { title: t('failure'), key: 'error', render: (_: unknown, item: Operation) => item.error ?? item.cancelError },
    { title: '', key: 'cancel', render: (_: unknown, item: Operation) => canWrite && item.state === 'Accepted' && item.request.kind === 'add'
      ? <Button onClick={() => run(() => api('admin/clusters/' + selected + '/cancel/' + item.operationId, {}))}>{t('cancel')}</Button> : null },
  ];
  const nodeTable = <Table rowKey={node => node.status.nodeId} columns={nodeColumns} dataSource={observation?.nodes ?? []} scroll={{ x: 900 }} pagination={false} />;
  const toolbar = <Space wrap>{canWrite && <><Button icon={<PlusOutlined />} disabled={activeOperation} onClick={() => open('add')}>{t('add')}</Button>
    <Button danger disabled={activeOperation || !status} onClick={() => open('remove')}>{t('remove')}</Button>
    <Button disabled={activeOperation || !status} onClick={() => operation('snapshot')}>{t('snapshot')}</Button></>}</Space>;
  const overview = <>
    <div className="metrics">{metrics.map(metric => <Card key={metric.title}><Tooltip title={metric.tooltip}><Text type="secondary">{metric.title}</Text></Tooltip>
      <Statistic value={metric.value} /><Text type="secondary" className="metric-foot">{metric.foot}</Text></Card>)}</div>
    <div className="overview-grid"><Card title={t('topology')} extra={<Text type="secondary">{t('term')} {status?.term ?? '—'}</Text>} className="topology">
      <Text type="secondary">{t('topologyHint')}</Text><div className="topology-canvas">
        {status?.leaderId && <div className="leader-position"><button className="node-card leader" onClick={() => setInspected(observation?.nodes.find(node => node.status.nodeId === status.leaderId))}>
          <DeploymentUnitOutlined /><b>{status.leaderId}</b>{roleTag('Leader')}<small>commit {number(status.commitIndex)}</small></button></div>}
        <div className="followers">{[...status?.voters ?? [], ...status?.learners ?? []].filter(id => id !== status?.leaderId).map(id => {
          const node = observation?.nodes.find(item => item.status.nodeId === id);
          return <button key={id} className="node-card" onClick={() => node && setInspected(node)}><DatabaseOutlined /><b>{id}</b>{roleTag(node?.status.role ?? 'Unknown')}
            <small>{t('applied')} {number(node?.status.lastApplied)}</small></button>;
        })}</div>{!status && <Empty description={t('empty')} />}</div>
      <div className="topology-foot"><Badge status={stale ? 'warning' : 'success'} text={t(stale ? 'stale' : 'live')} /><Text type="secondary">{t('quorum')} {status ? Math.floor(status.voters.length / 2) + 1 : '—'}</Text></div>
    </Card><Card title={t('events')}><div className="event-list">{(observation?.operations ?? []).slice(-6).reverse().map(item => <div key={item.operationId} className="event">
      <Badge status={item.state === 'Failed' ? 'error' : item.state === 'Accepted' ? 'processing' : 'success'} /><div><b>{item.request.kind} {item.request.nodeId}</b><p>{item.state}</p><small className="mono">{item.operationId.slice(0, 18)}…</small></div>
    </div>)}{!observation?.operations.length && <Empty description={t('empty')} image={Empty.PRESENTED_IMAGE_SIMPLE} />}</div></Card>
    <Card title={t('throughput')} extra={<Space wrap><Select value={historyRange} onChange={setHistoryRange} options={[{ value: 'live', label: 'Live' }, { value: 'day', label: '24 h' }, { value: 'week', label: '7 d' }]} />
      <Select value={chartMetric} onChange={setChartMetric} options={[{ value: 'messagesPerSecond', label: 'msg/s' }, { value: 'p99Milliseconds', label: 'p99' }]} /></Space>}>
      <Chart points={historyRange === 'live' ? observation?.history ?? [] : minuteHistory.filter(point => Date.now() - Date.parse(point.at) <= (historyRange === 'day' ? 1 : 7) * 86400000)} field={chartMetric} t={t} /><Text type="secondary">{t('noSynthetic')}</Text></Card>
    <Card title={t('resources')}><div className="storage-total">{bytes(last?.physicalStorageBytes)}</div>
      {[['log', 'logBytes'], ['history', 'messageBytes'], ['images', 'imageBytes']].map(([label, property]) => <div className="resource-row" key={label}>
        <Text type="secondary">{t(label as Label)}</Text><b className="mono">{freshNodes.length && freshNodes.every(node => node.resources[property as keyof typeof node.resources] != null) ? bytes(freshNodes.reduce((total, node) => total + Number(node.resources[property as keyof typeof node.resources] ?? 0), 0)) : '—'}</b></div>)}
      <Paragraph type="secondary" className="resource-note">{t('workingSet')}: {bytes(last?.workingSetBytes)}</Paragraph></Card></div>
    <Card title={t('nodes')} extra={toolbar}>{nodeTable}</Card>
  </>;
  function report() {
    const blob = new Blob([JSON.stringify({ exportedAt: new Date().toISOString(), cluster: observation }, null, 2)], { type: 'application/json' });
    const link = document.createElement('a'); const url = URL.createObjectURL(blob); link.href = url; link.download = 'raft-diagnostics.json'; link.click(); URL.revokeObjectURL(url);
  }
  return <>
    {!currentSession || currentSession.username && authorizedUser !== currentSession.username ? <div className="login"><Spin size="large" /></div> : !currentSession.username ? <div className="login"><Card className="login-card"><div className="brand"><span>R</span><div>RAFT / CONTROL<small>CLUSTER ADMINISTRATION</small></div></div>
      <Title level={3}>{t('signIn')}</Title><Form layout="vertical" onFinish={values => void run(async () => { await api('login', values); await refreshSession(); })}>
        <Form.Item name="username" label={t('username')} rules={[{ required: true }]}><Input autoComplete="username" /></Form.Item>
        <Form.Item name="password" label={t('password')} rules={[{ required: true }]}><Input.Password autoComplete="current-password" /></Form.Item>
        <Form.Item name="code" label={t('code')}><Input inputMode="numeric" autoComplete="one-time-code" /></Form.Item>
        <Form.Item name="recoveryCode" label={t('recovery')}><Input autoComplete="off" /></Form.Item>
        <Button block type="primary" htmlType="submit" loading={loading}>{t('signIn')}</Button></Form>
      <Button type="text" onClick={() => setLanguage(language === 'ru' ? 'en' : 'ru')}>RU / EN</Button></Card></div> :
      <Layout className="shell"><Layout.Sider width={248} breakpoint="lg" collapsedWidth={72} className="sidebar">
        <div className="brand"><span>R</span><div>RAFT / CONTROL<small>CLUSTER ADMINISTRATION</small></div></div>
        <div className="cluster-select"><Select aria-label={t('group')} value={selected || undefined} placeholder={t('noClusters')} onChange={setSelected}
          options={profiles.map(profile => ({ value: profile.id, label: profile.name }))} /></div>
        <Menu mode="inline" selectedKeys={[section]} onClick={({ key }) => navigate('/' + key)} items={[
          { key: 'overview', icon: <DashboardOutlined />, label: t('overview') }, { key: 'nodes', icon: <DatabaseOutlined />, label: t('nodes') },
          { key: 'entities', icon: <ApiOutlined />, label: t('entities') }, { key: 'replication', icon: <DeploymentUnitOutlined />, label: t('replication') },
          { key: 'operations', icon: <LineChartOutlined />, label: t('operations') }, { key: 'audit', icon: <FileTextOutlined />, label: t('audit') },
          ...(admin ? [{ key: 'tokens', icon: <SafetyCertificateOutlined />, label: t('tokens') }, { key: 'users', icon: <TeamOutlined />, label: t('users') }] : []),
          { key: 'settings', icon: <SettingOutlined />, label: t('settings') }]} />
        <div className="sidebar-bottom"><Text type="secondary">{currentSession.username} · {currentSession.role}</Text>
          <Button icon={<LogoutOutlined />} type="text" onClick={() => void run(async () => { await api('account/logout', {}); await refreshSession(); })}>{t('logout')}</Button></div>
      </Layout.Sider><Layout><header className="header"><div><ApartmentOutlined /><span>{observation?.name ?? 'RaftClusterDashboard'}</span><Text type="secondary">/ {t(section in labels ? section as Label : 'overview')}</Text></div>
        <Space><Select className="mobile-cluster" aria-label={t('group')} value={selected || undefined} onChange={setSelected} options={profiles.map(profile => ({ value: profile.id, label: profile.name }))} />
          <Badge status={streamConnected ? 'success' : 'warning'} text={t(streamConnected ? 'live' : 'disconnected')} />
          <Select aria-label={t('language')} value={language} onChange={setLanguage} options={[{ value: 'ru', label: 'RU' }, { value: 'en', label: 'EN' }]} />
          <Button aria-label={t(dark ? 'light' : 'dark')} icon={dark ? <SunOutlined /> : <MoonOutlined />} onClick={() => setDark(!dark)} /></Space></header>
        <main><div className="page-heading"><div><div className="eyebrow">RAFT / CLUSTER ADMIN</div><Title level={1}>{t(section === 'overview' ? 'title' : section in labels ? section as Label : 'overview')}</Title>
          <Text type="secondary">{t('subtitle')}</Text></div><Space wrap><Button onClick={report} disabled={!observation}>{t('report')}</Button>{admin && <Button type="primary" icon={<PlusOutlined />} onClick={() => open('cluster')}>{t('connect')}</Button>}</Space></div>
          {needsEnrollment ? <Alert type="warning" title={t('mfaRequired')} action={<Button onClick={() => open('mfa')}>{t('enroll')}</Button>} /> :
          !selected && section !== 'users' && section !== 'settings' ? <Card><Empty description={t('noClusters')}>{admin && <Button type="primary" onClick={() => open('cluster')}>{t('connect')}</Button>}</Empty></Card> :
          <>{stale && selected && <Alert className="stale-alert" type="warning" showIcon title={t('oldData')} description={observation?.error} />}
            {observation?.credentialExpiresAt && Date.parse(observation.credentialExpiresAt) - Date.now() < 7 * 86400000 && <Alert className="stale-alert" type="warning" showIcon title={t('expiringToken')} description={new Date(observation.credentialExpiresAt).toLocaleString()} />}
            {status?.peers.some(peer => BigInt(status.lastLogIndex) - BigInt(peer.matchIndex) > 1024n) && <Alert className="stale-alert" type="warning" showIcon title={t('lag')} description={t('replication')} />}
            {freshNodes.some(node => node.resources.volumeAvailableBytes != null && node.resources.volumeTotalBytes != null && Number(node.resources.volumeAvailableBytes) < Number(node.resources.volumeTotalBytes) * .1) && <Alert className="stale-alert" type="warning" showIcon title={t('lowDisk')} />}
            {section === 'overview' && overview}
            {section === 'nodes' && <Card title={t('nodes')} extra={toolbar}>{nodeTable}</Card>}
            {section === 'entities' && <Card extra={canWrite && <Button icon={<PlusOutlined />} onClick={() => open('createEntity')}>{t('create')}</Button>}>
              <Table rowKey={item => item.entity.name} dataSource={observation?.entities ?? []} scroll={{ x: 850 }} columns={[
                { title: t('name'), key: 'name', render: (_, item) => <b>{item.entity.name}</b> }, { title: t('mode'), key: 'mode', render: (_, item) => t(item.entity.mode === 0 ? 'standard' : 'latest') },
                { title: 'messageId', key: 'last', render: (_, item) => number(item.entity.lastMessageId) }, { title: t('messages'), key: 'count', render: (_, item) => number(item.entity.retainedMessages) },
                { title: 'msg/s', key: 'activity', render: (_, item) => number(item.messagesPerSecond) },
                { title: t('bytes'), key: 'size', render: (_, item) => bytes(item.entity.retainedBytes) }, { title: t('state'), key: 'state', render: (_, item) => <Tag color={item.settings.writesPaused ? 'gold' : 'green'}>{t(item.settings.writesPaused ? 'pause' : 'active')}</Tag> },
                { title: '', key: 'configure', render: (_, item) => <Space>
                  {canWrite && <Button onClick={() => { setEntity(item); open('entity', item.settings); }}>{t('configure')}</Button>}
                  {currentSession.canReadPayload && BigInt(item.entity.lastMessageId) > 0n && <Button onClick={() => { setEntity(item); open('preview', { messageId: String(item.entity.lastMessageId) }); }}>{t('preview')}</Button>}
                </Space> }]} /></Card>}
            {section === 'replication' && <Card title={t('replication')}><Table rowKey="nodeId" dataSource={status?.peers ?? []} scroll={{ x: 650 }} columns={[
              { title: t('nodeId'), dataIndex: 'nodeId' }, { title: t('mode'), dataIndex: 'replicationMode' }, { title: 'MatchIndex', dataIndex: 'matchIndex' }, { title: 'NextIndex', dataIndex: 'nextIndex' },
              { title: 'In flight', dataIndex: 'inFlightRequests' }, { title: 'In flight bytes', dataIndex: 'inFlightBytes', render: bytes }, { title: 'Snapshot offset', dataIndex: 'snapshotOffset', render: bytes }]} /></Card>}
            {section === 'operations' && <Card><Table rowKey="operationId" columns={operationColumns} dataSource={[...observation?.operations ?? []].reverse()} scroll={{ x: 850 }} /></Card>}
            {section === 'tokens' && admin && <Card extra={<Button type="primary" onClick={() => { setEditingToken(undefined); open('token', { permissions: [1, 2, 4, 8], entities: '*', expiresAt: localDateTime(new Date(Date.now() + 30 * 86400000)) }); }}>{t('issue')}</Button>}>
              <Table rowKey="tokenId" dataSource={tokens} scroll={{ x: 850 }} columns={[
                { title: t('name'), key: 'name', render: (_, token) => token.grant.name }, { title: 'ID', dataIndex: 'tokenId', render: (value: string) => <Text copyable>{value}</Text> },
                { title: t('allowedEntities'), key: 'entities', render: (_, token) => token.grant.entities.join(', ') }, { title: t('expires'), key: 'expiry', render: (_, token) => new Date(token.grant.expiresAt).toLocaleString() },
                { title: t('state'), key: 'revoked', render: (_, token) => <Tag color={token.revoked ? 'default' : Date.parse(token.grant.expiresAt) <= Date.now() ? 'red' : 'green'}>{t(token.revoked ? 'revoked' : Date.parse(token.grant.expiresAt) <= Date.now() ? 'expired' : 'active')}</Tag> },
                { title: '', key: 'revoke', render: (_, token) => <Space><Button disabled={token.revoked} onClick={() => { setEditingToken(token); open('token', {
                  name: token.grant.name, entities: token.grant.entities.join(', '), expiresAt: localDateTime(new Date(token.grant.expiresAt)),
                  permissions: [1,2,4,8,16,32,64,128,256].filter(bit => (token.grant.permissions & bit) !== 0) }); }}>{t('editToken')}</Button>
                  <Button danger disabled={token.revoked} onClick={() => modal.confirm({ title: t('revoke'), content: token.grant.name,
                  onOk: () => run(async () => { await api('admin/clusters/' + selected + '/tokens/' + token.tokenId + '/revoke/' + operationId(), {}); setTokens(await api('admin/clusters/' + selected + '/tokens')); }) })}>{t('revoke')}</Button></Space> }]} /></Card>}
            {section === 'audit' && <Card extra={admin && <Select value={auditSource} onChange={setAuditSource} options={[{ value: 'cluster', label: 'Cluster' }, { value: 'dashboard', label: 'Dashboard' }]} />}><Table rowKey={(item, index) => String(item.index ?? item.at) + index} dataSource={audit} scroll={{ x: 650 }} columns={[
              { title: 'Index / UTC', key: 'index', render: (_, item) => String(item.index ?? item.at ?? '') }, { title: t('source'), key: 'actor', render: (_, item) => String(item.actor ?? item.userId ?? '') }, { title: t('name'), dataIndex: 'action' }, { title: t('endpoint'), dataIndex: 'target' }, { title: t('failure'), dataIndex: 'error' }]} /></Card>}
            {section === 'users' && admin && <Card title={t('users')} extra={<Button onClick={() => { setEditingUser(undefined); open('user', { role: 'Viewer', clusterIds: [] }); }}>{t('newUser')}</Button>}>
              <Paragraph type="secondary">{t('permissionsHint')}</Paragraph><Table rowKey="id" dataSource={users} columns={[
                { title: t('username'), dataIndex: 'userName' }, { title: t('role'), dataIndex: 'role' }, { title: t('allowedClusters'), dataIndex: 'clusterIds', render: (ids: string[]) => ids.map(id => profiles.find(profile => profile.id === id)?.name ?? id).join(', ') },
                { title: 'TOTP', dataIndex: 'twoFactorEnabled', render: (value: boolean) => <Badge status={value ? 'success' : 'default'} /> },
                { title: '', key: 'edit', render: (_, user) => <Button onClick={() => { setEditingUser(user); open('user', { username: user.userName, role: user.role, clusterIds: user.clusterIds, canReadPayload: user.canReadPayload }); }}>{t('editUser')}</Button> }]} /></Card>}
            {section === 'settings' && <Card title={t('settings')} extra={admin && selected && <Button onClick={() => { const profile = profiles.find(item => item.id === selected)!; open('cluster', { ...profile, endpoints: profile.managementEndpoints.join('\n') }); }}>{t('configure')}</Button>}><Alert type="info" title={t('configHint')} /><Descriptions bordered column={1} className="settings-description" items={[
              { key: 'group', label: t('group'), children: status?.groupId ?? '—' }, { key: 'version', label: 'Application', children: status?.applicationVersion ?? '—' },
              { key: 'config', label: 'Configuration index', children: status?.configurationIndex ?? '—' }, { key: 'snapshot', label: 'Snapshot index', children: status?.snapshotIndex ?? '—' },
              { key: 'auth', label: t('mfa'), children: <Space>{currentSession.mfaEnabled ? <Tag color="green">TOTP</Tag> : '—'}<Button onClick={() => open('mfa')}>{t('enroll')}</Button></Space> }]} />
              <Descriptions bordered column={1} className="settings-description" items={Object.entries(configuration).map(([key, value]) => ({ key, label: key, children: value }))} /></Card>}
          </>}</main><footer><span>{t('noSynthetic')} · {observation?.observedAt ? new Date(observation.observedAt).toLocaleString() : '—'}</span><span>RaftClusterDashboard / .NET 8</span></footer>
      </Layout></Layout>}
    <Drawer width={540} title={inspected?.status.nodeId} open={!!inspected && ownsView} onClose={() => setInspected(undefined)}>
      {inspected && <Descriptions bordered column={1} items={[
        { key: 'role', label: t('role'), children: roleTag(inspected.status.role) }, { key: 'term', label: t('term'), children: inspected.status.term },
        { key: 'commit', label: 'Commit / applied', children: number(inspected.status.commitIndex) + ' / ' + number(inspected.status.lastApplied) },
        { key: 'seen', label: t('lastSeen'), children: new Date(inspected.resources.observedAt).toLocaleString() },
        { key: 'host', label: 'Host', children: inspected.resources.hostId }, { key: 'ram', label: t('workingSet'), children: bytes(inspected.resources.workingSetBytes) },
        { key: 'private', label: 'Private / GC / cache', children: [inspected.resources.privateBytes, inspected.resources.gcHeapBytes, inspected.resources.cachedPayloadBytes].map(bytes).join(' / ') },
        { key: 'hostRam', label: t('hostRam'), children: bytes(inspected.resources.hostAvailableMemoryBytes) + ' / ' + bytes(inspected.resources.hostTotalMemoryBytes) },
        { key: 'volume', label: t('volume'), children: bytes(inspected.resources.volumeAvailableBytes) + ' / ' + bytes(inspected.resources.volumeTotalBytes) },
        { key: 'log', label: t('log'), children: bytes(inspected.resources.logBytes) }, { key: 'history', label: t('history'), children: bytes(inspected.resources.messageBytes) },
        { key: 'images', label: t('images'), children: bytes(inspected.resources.imageBytes) }, { key: 'fault', label: t('failure'), children: inspected.status.fault ?? '—' },
        { key: 'httpFailures', label: t('httpFailures'), children: number(inspected.failedRequests) },
        ...Object.entries(inspected.coreMetrics ?? {}).map(([key, metric]) => ({ key, label: key,
          children: key.endsWith('.duration') ? number(Number(metric.samples) > 0 ? metric.total / Number(metric.samples) : null) + ' ms (mean)' : number(metric.total) }))]} />}
    </Drawer>
    <Modal open={!!dialog && ownsView} destroyOnHidden onCancel={() => setDialog(undefined)} footer={null} title={t(dialog === 'cluster' ? 'connect' : dialog === 'add' ? 'add' :
      dialog === 'remove' ? 'remove' : dialog === 'preview' ? 'preview' :
      dialog === 'entity' ? 'configure' : dialog === 'createEntity' ? 'create' : dialog === 'token' ? editingToken ? 'editToken' : 'issue' : dialog === 'user' ? editingUser ? 'editUser' : 'newUser' : 'mfa')}>
      <Form form={form} layout="vertical" onFinish={values => void run(async () => {
        const root = 'admin/clusters/' + selected;
        if (dialog === 'cluster') { await api('admin/clusters', { ...values, managementEndpoints: values.endpoints.split(/\r?\n/).map((line: string) => line.trim()).filter(Boolean) }); await refreshProfiles(); }
        if (dialog === 'add') await api(root + '/operations', { operation: { kind: 'add', add: {
          peer: { nodeId: values.nodeId, host: values.host, port: values.port }, clientEndpoint: { nodeId: values.nodeId, address: values.client },
          managementEndpoint: { nodeId: values.nodeId, address: values.management } } }, operationId: dialogOperation });
        if (dialog === 'remove') await api(root + '/operations', { operation: { kind: 'remove', nodeId: values.nodeId }, operationId: dialogOperation });
        if (dialog === 'preview') setPreview(await api(root + '/preview?entity=' + encodeURIComponent(entity!.entity.name) + '&messageId=' + values.messageId));
        if (dialog === 'createEntity') await api(root + '/entities', { name: values.name, mode: values.mode, operationId: dialogOperation });
        if (dialog === 'entity') await api(root + '/entity-settings', { entity: entity!.entity.name, settings: { ...values, maxRetainedBytes: values.maxRetainedBytes ?? null }, operationId: dialogOperation });
        if (dialog === 'token') {
          const grant = { name: values.name, permissions: values.permissions.reduce((sum: number, bit: number) => sum | bit, 0),
            entities: values.entities.split(',').map((name: string) => name.trim()).filter(Boolean), expiresAt: new Date(values.expiresAt).toISOString() };
          if (editingToken) await api(root + '/tokens/update/' + dialogOperation, { ...editingToken, grant });
          else {
            const issued = await api<{ secret?: string }>(root + '/tokens', { grant, operationId: dialogOperation });
            setSecret(issued.secret);
          }
          setTokens(await api(root + '/tokens'));
        }
        if (dialog === 'user') { await api('admin/users', { ...values, id: editingUser?.id }); setUsers(await api('admin/users')); }
        if (dialog === 'mfa' && !authenticator) { const result = await api<{ key: string }>('account/mfa/start', values); setAuthenticator(result.key); await refreshSession(); setDialog('mfa'); return; }
        if (dialog === 'mfa' && authenticator) { const result = await api<{ recoveryCodes: string[] }>('account/mfa/confirm', values); setRecoveryCodes(result.recoveryCodes); setAuthenticator(undefined); await refreshSession(); }
      }, false, !(dialog === 'mfa' && !authenticator))}>
        {dialog === 'cluster' && <><Form.Item name="id" hidden><Input /></Form.Item><Form.Item name="name" label={t('name')} rules={[{ required: true }]}><Input /></Form.Item><Form.Item name="groupId" label={t('group')} rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="endpoints" label={t('endpoints')} rules={[{ required: true }]}><Input.TextArea rows={3} placeholder="http://127.0.0.1:7201/" /></Form.Item>
          <Form.Item name="credential" label={t('credential')} rules={[{ required: !form.getFieldValue('id') }]}><Input.Password autoComplete="off" /></Form.Item><Form.Item name="certificateAlias" label={t('certificate')}><Input /></Form.Item></>}
        {dialog === 'add' && <><Alert type="info" title={t('dangerous')} />{[['nodeId', 'nodeId'], ['host', 'peerHost'], ['client', 'clientEndpoint'], ['management', 'adminEndpoint']].map(([key, label]) =>
          <Form.Item key={key} name={key} label={t(label as Label)} rules={[{ required: true }]}><Input /></Form.Item>)}<Form.Item name="port" label={t('peerPort')} rules={[{ required: true }]}><InputNumber min={1} max={65535} /></Form.Item></>}
        {dialog === 'remove' && <><Alert type="warning" title={t('dangerous')} /><Form.Item name="nodeId" label={t('nodeId')} rules={[{ required: true }]}>
          <Select options={[...status?.voters ?? [], ...status?.learners ?? []].map(value => ({ value, label: value }))} /></Form.Item></>}
        {dialog === 'preview' && <><Paragraph>{t('previewLimit')}</Paragraph><Form.Item name="messageId" label="messageId" rules={[{ required: true }]}><InputNumber stringMode min="1" max="9223372036854775807" precision={0} /></Form.Item></>}
        {dialog === 'createEntity' && <><Form.Item name="name" label={t('name')} rules={[{ required: true }]}><Input /></Form.Item><Form.Item name="mode" label={t('mode')} initialValue={0}>
          <Select options={[{ value: 0, label: t('standard') }, { value: 1, label: t('latest') }]} /></Form.Item></>}
        {dialog === 'entity' && <><Form.Item name="writesPaused" label={t('pause')} valuePropName="checked"><Switch /></Form.Item><Form.Item name="maxPayloadBytes" label={t('maxPayload')} rules={[{ required: true }]}><InputNumber min={0} max={16000000} style={{ width: '100%' }} /></Form.Item>
          <Form.Item name="maxRetainedBytes" label={t('quota')} extra={t('quotaHint')}><InputNumber stringMode min="0" max="9223372036854775807" style={{ width: '100%' }} /></Form.Item></>}
        {dialog === 'token' && <><Form.Item name="name" label={t('name')} rules={[{ required: true }]}><Input /></Form.Item><Form.Item name="entities" label={t('allowedEntities')} rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="expiresAt" label={t('expires')} rules={[{ required: true }]}><Input type="datetime-local" /></Form.Item><Form.Item name="permissions" label={t('permissions')} rules={[{ required: true }]}><Checkbox.Group options={[
            { label: 'Discover', value: 1 }, { label: 'Publish', value: 2 }, { label: 'Read metadata', value: 4 }, { label: 'Read payload', value: 8 }, { label: 'Create entity', value: 16 },
            { label: 'Observe', value: 32 }, { label: 'Manage entities', value: 64 }, { label: 'Manage nodes', value: 128 }, { label: 'Manage tokens', value: 256 }]} /></Form.Item></>}
        {dialog === 'user' && <><Form.Item name="username" label={t('username')} rules={[{ required: true }]}><Input /></Form.Item><Form.Item name="password" label={t('password')} rules={[{ required: !editingUser, min: 12 }]}><Input.Password autoComplete="new-password" /></Form.Item>
          <Form.Item name="role" label={t('role')}><Select options={['Admin', 'Operator', 'Viewer'].map(value => ({ value, label: value }))} /></Form.Item>
          <Form.Item name="clusterIds" label={t('allowedClusters')}><Select mode="multiple" options={profiles.map(profile => ({ value: profile.id, label: profile.name }))} /></Form.Item></>}
        {dialog === 'user' && <Form.Item name="canReadPayload" valuePropName="checked"><Checkbox>{t('payloadPermission')}</Checkbox></Form.Item>}
        {dialog === 'mfa' && <>{authenticator ? <><QRCode value={'otpauth://totp/RaftDashboard:' + encodeURIComponent(currentSession?.username ?? '') + '?secret=' + authenticator + '&issuer=RaftDashboard'} />
          <Text copyable className="mono">{authenticator}</Text></> : <Form.Item name="password" label={t('password')} rules={[{ required: true }]}><Input.Password autoComplete="current-password" /></Form.Item>}
          {(authenticator || currentSession?.mfaEnabled) && <Form.Item name="code" label={t('code')} rules={[{ required: true }]}><Input inputMode="numeric" autoComplete="one-time-code" /></Form.Item>}</>}
        {['add', 'remove', 'token', 'entity', 'createEntity'].includes(dialog ?? '') && <Paragraph type="secondary" className="mono"><Text copyable>{dialogOperation}</Text></Paragraph>}
        <Button type="primary" block htmlType="submit" loading={loading}>{t(dialog === 'mfa' ? authenticator ? 'confirmMfa' : 'startMfa' : 'save')}</Button>
      </Form>
    </Modal>
    <Modal open={!!secret && ownsView} title={t('secretOnce')} onCancel={() => setSecret(undefined)} footer={<Button onClick={() => setSecret(undefined)}>{t('close')}</Button>}>
      <Input.TextArea value={secret} readOnly autoComplete="off" rows={4} /></Modal>
    <Modal open={!!recoveryCodes && ownsView} title={t('recoveryOnce')} onCancel={() => setRecoveryCodes(undefined)} footer={<Button onClick={() => setRecoveryCodes(undefined)}>{t('close')}</Button>}>
      <pre>{recoveryCodes?.join('\n')}</pre></Modal>
    <Modal open={!!preview && ownsView} title={t('preview')} onCancel={() => setPreview(undefined)} footer={<Button onClick={() => setPreview(undefined)}>{t('close')}</Button>}>
      {preview && <><Paragraph>{preview.messageId} · {bytes(preview.payloadLength)} {preview.truncated && <Tag>{t('truncated')}</Tag>}</Paragraph><Text type="secondary">{t('previewLimit')}</Text>
        <pre className="payload-preview">{new TextDecoder().decode(Uint8Array.from(atob(preview.base64), character => character.charCodeAt(0)))}</pre>
        <Input.TextArea value={preview.base64} readOnly rows={3} aria-label="Base64" /></>}
    </Modal>
  </>;
}

createRoot(document.getElementById('root')!).render(<React.StrictMode><BrowserRouter><DashboardTheme /></BrowserRouter></React.StrictMode>);
