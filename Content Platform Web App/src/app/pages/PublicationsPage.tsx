import { useEffect, useMemo, useState } from 'react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table';
import { Badge } from '../components/ui/badge';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '../components/ui/dialog';
import { Search, RefreshCw, AlertCircle, CheckCircle2, Clock } from 'lucide-react';
import { toast } from 'sonner';
import { api } from '../lib/api';

interface PostRow {
  id: number;
  title: string;
  text_body: string;
  status: string;
  scheduled_for?: string | null;
  updated_at: string;
  preview_payload?: any;
}

export function PublicationsPage() {
  const [posts, setPosts] = useState<PostRow[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [channelFilter, setChannelFilter] = useState<string>('all');
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [selectedPublication, setSelectedPublication] = useState<PostRow | null>(null);

  const loadPosts = async () => {
    try {
      const rows = await api<PostRow[]>('/api/posts?limit=500');
      setPosts(rows);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  useEffect(() => { loadPosts(); }, []);

  const filtered = useMemo(() => {
    return posts.filter((p) => {
      const hay = `${p.title} ${p.text_body}`.toLowerCase();
      const matchesSearch = !searchQuery.trim() || hay.includes(searchQuery.toLowerCase());
      const matchesStatus = statusFilter === 'all' || p.status === statusFilter;
      const channel = (p.preview_payload?.published?.channels?.[0] || 'telegram') as string;
      const matchesChannel = channelFilter === 'all' || channel === channelFilter;
      return matchesSearch && matchesStatus && matchesChannel;
    });
  }, [posts, searchQuery, statusFilter, channelFilter]);

  const handleRetry = async (id: number) => {
    try {
      await api(`/api/posts/${id}/publish`, 'POST', { mode: 'now' });
      toast.success('Повторная отправка выполнена');
      await loadPosts();
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const getStatusIcon = (status: string) => {
    if (status === 'published') return <CheckCircle2 className="h-4 w-4 text-green-400" />;
    if (status === 'scheduled') return <Clock className="h-4 w-4 text-yellow-400" />;
    if (status === 'cancelled' || status === 'failed') return <AlertCircle className="h-4 w-4 text-red-400" />;
    return <Clock className="h-4 w-4 text-zinc-400" />;
  };

  const getStatusBadge = (status: string) => {
    const map: Record<string, string> = {
      published: 'bg-green-950/30 text-green-400 border-green-900/50',
      scheduled: 'bg-yellow-950/30 text-yellow-400 border-yellow-900/50',
      failed: 'bg-red-950/30 text-red-400 border-red-900/50',
      cancelled: 'bg-red-950/30 text-red-400 border-red-900/50',
      preview_ready: 'bg-blue-950/30 text-blue-400 border-blue-900/50',
      draft: 'bg-zinc-800 text-zinc-300 border-zinc-700',
    };
    return <Badge variant="outline" className={map[status] || map.draft}>{status}</Badge>;
  };

  const publishedCount = posts.filter((p) => p.status === 'published').length;
  const queuedCount = posts.filter((p) => p.status === 'scheduled').length;
  const failedCount = posts.filter((p) => p.status === 'failed' || p.status === 'cancelled').length;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400"><span>Главная</span><span>/</span><span className="text-zinc-200">Публикации</span></div>

      <div>
        <h1 className="text-3xl font-bold text-zinc-50">Публикации</h1>
        <p className="text-zinc-400 mt-1">Всего: {posts.length} | Опубликовано: {publishedCount} | В очереди: {queuedCount} | Ошибки: {failedCount}</p>
      </div>

      <div className="flex flex-col gap-4 sm:flex-row bg-zinc-900 p-4 rounded-lg border border-zinc-800">
        <div className="flex-1 relative"><Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-500" /><Input placeholder="Поиск по содержимому..." value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} className="pl-9 bg-zinc-800 border-zinc-700 text-zinc-100 placeholder:text-zinc-500" /></div>
        <Button variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-200" onClick={loadPosts}>Обновить</Button>
        <Select value={channelFilter} onValueChange={setChannelFilter}>
          <SelectTrigger className="w-full sm:w-[180px] bg-zinc-800 border-zinc-700 text-zinc-200"><SelectValue placeholder="Канал" /></SelectTrigger>
          <SelectContent className="bg-zinc-900 border-zinc-800">
            <SelectItem value="all" className="text-zinc-200">Все каналы</SelectItem>
            <SelectItem value="telegram" className="text-zinc-200">Telegram</SelectItem>
            <SelectItem value="vk" className="text-zinc-200">VK</SelectItem>
            <SelectItem value="max" className="text-zinc-200">MAX</SelectItem>
            <SelectItem value="pinterest" className="text-zinc-200">Pinterest</SelectItem>
            <SelectItem value="instagram" className="text-zinc-200">Instagram</SelectItem>
          </SelectContent>
        </Select>
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-full sm:w-[180px] bg-zinc-800 border-zinc-700 text-zinc-200"><SelectValue placeholder="Статус" /></SelectTrigger>
          <SelectContent className="bg-zinc-900 border-zinc-800">
            <SelectItem value="all" className="text-zinc-200">Все статусы</SelectItem>
            <SelectItem value="published" className="text-zinc-200">published</SelectItem>
            <SelectItem value="scheduled" className="text-zinc-200">scheduled</SelectItem>
            <SelectItem value="failed" className="text-zinc-200">failed</SelectItem>
            <SelectItem value="preview_ready" className="text-zinc-200">preview_ready</SelectItem>
            <SelectItem value="draft" className="text-zinc-200">draft</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="bg-zinc-900 rounded-lg border border-zinc-800 overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-zinc-800/50 border-zinc-800">
              <TableHead className="text-zinc-300">Содержимое</TableHead>
              <TableHead className="text-zinc-300">Канал</TableHead>
              <TableHead className="text-zinc-300">Статус</TableHead>
              <TableHead className="text-zinc-300">Дата и время</TableHead>
              <TableHead className="text-zinc-300">Действия</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filtered.length === 0 ? <TableRow><TableCell colSpan={5} className="text-center py-12 text-zinc-500">Публикации не найдены</TableCell></TableRow> : filtered.map((pub) => (
              <TableRow key={pub.id} className="hover:bg-zinc-800/50 border-zinc-800">
                <TableCell className="text-zinc-200 max-w-md truncate">{pub.title || pub.text_body}</TableCell>
                <TableCell>
                  <Badge variant="outline" className="bg-blue-950/30 text-blue-400 border-blue-900/50">
                    {String(pub.preview_payload?.published?.channels?.[0] || 'telegram')}
                  </Badge>
                </TableCell>
                <TableCell><div className="flex items-center gap-2">{getStatusIcon(pub.status)}{getStatusBadge(pub.status)}</div></TableCell>
                <TableCell className="text-zinc-400">{(pub.scheduled_for || pub.updated_at || '').replace('T', ' ').slice(0, 16)}</TableCell>
                <TableCell>
                  <div className="flex items-center gap-2">
                    <Button variant="ghost" size="sm" onClick={() => setSelectedPublication(pub)} className="text-zinc-400 hover:text-zinc-200">Подробнее</Button>
                    {(pub.status === 'failed' || pub.status === 'cancelled') && <Button variant="ghost" size="sm" onClick={() => handleRetry(pub.id)} className="text-blue-400 hover:text-blue-300"><RefreshCw className="h-4 w-4" /></Button>}
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      <Dialog open={!!selectedPublication} onOpenChange={() => setSelectedPublication(null)}>
        <DialogContent className="bg-zinc-900 border-zinc-800 max-w-2xl">
          <DialogHeader>
            <DialogTitle className="text-zinc-50">Детали публикации</DialogTitle>
            <DialogDescription className="text-zinc-400">ID: {selectedPublication?.id}</DialogDescription>
          </DialogHeader>
          {selectedPublication && (
            <div className="space-y-4">
              <div><Label className="text-sm text-zinc-400">Содержимое</Label><p className="text-zinc-200 mt-1 whitespace-pre-wrap">{selectedPublication.text_body}</p></div>
              <div><Label className="text-sm text-zinc-400">Статус</Label><div className="mt-1">{getStatusBadge(selectedPublication.status)}</div></div>
              <div><Label className="text-sm text-zinc-400">Дата и время</Label><p className="text-zinc-200 mt-1">{(selectedPublication.scheduled_for || selectedPublication.updated_at || '').replace('T', ' ').slice(0, 16)} (GMT+3)</p></div>
              {(selectedPublication.status === 'failed' || selectedPublication.status === 'cancelled') && <Button onClick={() => handleRetry(selectedPublication.id)} className="w-full bg-blue-600 hover:bg-blue-700 text-white"><RefreshCw className="mr-2 h-4 w-4" />Повторить отправку</Button>}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}

function Label({ children, className }: { children: React.ReactNode; className?: string }) {
  return <label className={className}>{children}</label>;
}
