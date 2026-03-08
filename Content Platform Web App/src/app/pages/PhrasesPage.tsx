import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../components/ui/table';
import { Badge } from '../components/ui/badge';
import { Checkbox } from '../components/ui/checkbox';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '../components/ui/alert-dialog';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '../components/ui/dropdown-menu';
import { Upload, Search, MoreVertical, Trash2, Check, Sparkles } from 'lucide-react';
import { toast } from 'sonner';
import { api } from '../lib/api';

interface Phrase {
  id: number;
  text_body: string;
  topic?: string | null;
  is_published: number;
  created_at: string;
}

export function PhrasesPage() {
  const navigate = useNavigate();
  const [phrases, setPhrases] = useState<Phrase[]>([]);
  const [selectedPhrases, setSelectedPhrases] = useState<number[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | '0' | '1'>('all');
  const [themeFilter, setThemeFilter] = useState<string>('all');
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  const loadPhrases = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('limit', '2000');
      if (statusFilter !== 'all') params.set('status', statusFilter);
      if (searchQuery.trim()) params.set('search', searchQuery.trim());
      if (themeFilter !== 'all') params.set('topic', themeFilter);
      const rows = await api<Phrase[]>(`/api/phrases?${params.toString()}`);
      setPhrases(rows);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadPhrases();
  }, [statusFilter, themeFilter]);

  const filteredPhrases = phrases.filter((phrase) =>
    !searchQuery.trim() || phrase.text_body.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const themes = Array.from(new Set(phrases.map((p) => (p.topic || '').trim()).filter(Boolean)));

  const handleSelectAll = (checked: boolean) => {
    if (checked) setSelectedPhrases(filteredPhrases.map((p) => p.id));
    else setSelectedPhrases([]);
  };

  const handleSelectPhrase = (id: number, checked: boolean) => {
    if (checked) setSelectedPhrases([...selectedPhrases, id]);
    else setSelectedPhrases(selectedPhrases.filter((pId) => pId !== id));
  };

  const handleMarkAsPublished = async () => {
    if (!selectedPhrases.length) return;
    try {
      await api('/api/phrases/bulk-status', 'PUT', { ids: selectedPhrases, is_published: 1 });
      toast.success(`Отмечено как опубликованные: ${selectedPhrases.length}`);
      setSelectedPhrases([]);
      await loadPhrases();
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const handleDelete = async () => {
    try {
      await api('/api/phrases/bulk-delete', 'DELETE', { ids: selectedPhrases });
      toast.success(`Удалено фраз: ${selectedPhrases.length}`);
      setSelectedPhrases([]);
      setDeleteDialogOpen(false);
      await loadPhrases();
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const handleGenerateForPhrase = (id: number) => navigate(`/phrases/generate?phraseId=${id}`);

  const newPhrasesCount = phrases.filter((p) => p.is_published === 0).length;
  const publishedPhrasesCount = phrases.filter((p) => p.is_published === 1).length;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400"><span>Главная</span><span>/</span><span className="text-zinc-200">Фразы</span></div>

      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-zinc-50">Фразы</h1>
          <p className="text-zinc-400 mt-1">Всего: {phrases.length} | Новые: {newPhrasesCount} | Опубликованные: {publishedPhrasesCount}</p>
        </div>
        <div className="flex gap-2">
          <Link to="/phrases/import"><Button className="bg-zinc-800 hover:bg-zinc-700 text-zinc-200"><Upload className="mr-2 h-4 w-4" />Импорт</Button></Link>
          <Link to="/phrases/generate"><Button className="bg-blue-600 hover:bg-blue-700 text-white"><Sparkles className="mr-2 h-4 w-4" />Генерировать пост</Button></Link>
        </div>
      </div>

      <div className="flex flex-col gap-4 sm:flex-row bg-zinc-900 p-4 rounded-lg border border-zinc-800">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-500" />
          <Input placeholder="Поиск по тексту..." value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} className="pl-9 bg-zinc-800 border-zinc-700 text-zinc-100 placeholder:text-zinc-500" />
        </div>
        <Button variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-200" onClick={loadPhrases}>Поиск</Button>
        <Select value={statusFilter} onValueChange={(v) => setStatusFilter(v as any)}>
          <SelectTrigger className="w-full sm:w-[180px] bg-zinc-800 border-zinc-700 text-zinc-200"><SelectValue placeholder="Статус" /></SelectTrigger>
          <SelectContent className="bg-zinc-900 border-zinc-800">
            <SelectItem value="all" className="text-zinc-200">Все</SelectItem>
            <SelectItem value="0" className="text-zinc-200">Новые ({newPhrasesCount})</SelectItem>
            <SelectItem value="1" className="text-zinc-200">Опубликованные ({publishedPhrasesCount})</SelectItem>
          </SelectContent>
        </Select>
        <Select value={themeFilter} onValueChange={setThemeFilter}>
          <SelectTrigger className="w-full sm:w-[180px] bg-zinc-800 border-zinc-700 text-zinc-200"><SelectValue placeholder="Тема" /></SelectTrigger>
          <SelectContent className="bg-zinc-900 border-zinc-800">
            <SelectItem value="all" className="text-zinc-200">Все темы</SelectItem>
            {themes.map((theme) => <SelectItem key={theme} value={theme} className="text-zinc-200">{theme}</SelectItem>)}
          </SelectContent>
        </Select>
      </div>

      {selectedPhrases.length > 0 && (
        <div className="flex items-center gap-3 bg-blue-950/30 border border-blue-900/50 px-4 py-3 rounded-lg">
          <span className="text-sm text-zinc-200">Выбрано: {selectedPhrases.length}</span>
          <div className="flex gap-2 ml-auto">
            <Button size="sm" variant="outline" onClick={handleMarkAsPublished} className="bg-green-950/30 border-green-900/50 text-green-400 hover:bg-green-900/40"><Check className="mr-2 h-4 w-4" />Отметить опубликованными</Button>
            <Button size="sm" variant="outline" onClick={() => setDeleteDialogOpen(true)} className="bg-red-950/30 border-red-900/50 text-red-400 hover:bg-red-900/40"><Trash2 className="mr-2 h-4 w-4" />Удалить</Button>
          </div>
        </div>
      )}

      <div className="bg-zinc-900 rounded-lg border border-zinc-800 overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-zinc-800/50 border-zinc-800">
              <TableHead className="w-12"><Checkbox checked={selectedPhrases.length === filteredPhrases.length && filteredPhrases.length > 0} onCheckedChange={handleSelectAll} className="border-zinc-700" /></TableHead>
              <TableHead className="text-zinc-300">Текст</TableHead>
              <TableHead className="text-zinc-300">Тема</TableHead>
              <TableHead className="text-zinc-300">Статус</TableHead>
              <TableHead className="text-zinc-300">Дата добавления</TableHead>
              <TableHead className="w-12"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow><TableCell colSpan={6} className="text-center py-12 text-zinc-500">Загрузка...</TableCell></TableRow>
            ) : filteredPhrases.length === 0 ? (
              <TableRow><TableCell colSpan={6} className="text-center py-12 text-zinc-500">Фразы не найдены</TableCell></TableRow>
            ) : (
              filteredPhrases.map((phrase) => (
                <TableRow key={phrase.id} className="hover:bg-zinc-800/50 border-zinc-800">
                  <TableCell><Checkbox checked={selectedPhrases.includes(phrase.id)} onCheckedChange={(checked) => handleSelectPhrase(phrase.id, checked as boolean)} className="border-zinc-700" /></TableCell>
                  <TableCell className="text-zinc-200 max-w-md">{phrase.text_body}</TableCell>
                  <TableCell className="text-zinc-400">{phrase.topic || '—'}</TableCell>
                  <TableCell>
                    <Badge variant={phrase.is_published === 1 ? 'default' : 'secondary'} className={phrase.is_published === 1 ? 'bg-green-950/30 text-green-400 border-green-900/50' : 'bg-yellow-950/30 text-yellow-400 border-yellow-900/50'}>
                      {phrase.is_published === 1 ? 'Опубликована' : 'Новая'}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-zinc-400">{(phrase.created_at || '').slice(0, 10)}</TableCell>
                  <TableCell>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button variant="ghost" size="sm" className="text-zinc-400 hover:text-zinc-200"><MoreVertical className="h-4 w-4" /></Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end" className="bg-zinc-900 border-zinc-800">
                        <DropdownMenuItem className="text-zinc-300 focus:bg-zinc-800" onClick={() => handleGenerateForPhrase(phrase.id)}>Генерировать пост</DropdownMenuItem>
                        <DropdownMenuItem className="text-red-400 focus:bg-zinc-800" onClick={async () => { await api(`/api/phrases/${phrase.id}`, 'DELETE', {}); await loadPhrases(); }}>Удалить</DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent className="bg-zinc-900 border-zinc-800">
          <AlertDialogHeader>
            <AlertDialogTitle className="text-zinc-50">Удалить фразы?</AlertDialogTitle>
            <AlertDialogDescription className="text-zinc-400">Вы уверены, что хотите удалить выбранные фразы ({selectedPhrases.length})?</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700">Отмена</AlertDialogCancel>
            <AlertDialogAction onClick={handleDelete} className="bg-red-600 hover:bg-red-700 text-white">Удалить</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
