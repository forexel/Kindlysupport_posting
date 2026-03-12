import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
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
import { Textarea } from '../components/ui/textarea';
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
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '../components/ui/dialog';
import { Upload, Search, MoreVertical, Trash2, Check, Sparkles, WandSparkles } from 'lucide-react';
import { toast } from 'sonner';
import { api } from '../lib/api';

interface Phrase {
  id: number;
  text_body: string;
  author?: string | null;
  is_published: number;
  created_at: string;
}

export function PhrasesPage() {
  const navigate = useNavigate();
  const [phrases, setPhrases] = useState<Phrase[]>([]);
  const [selectedPhrases, setSelectedPhrases] = useState<number[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | '0' | '1'>('0');
  const [sortBy, setSortBy] = useState<'text' | 'created_at'>('created_at');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [editingPhraseId, setEditingPhraseId] = useState<number | null>(null);
  const [editingText, setEditingText] = useState('');
  const [editingAuthor, setEditingAuthor] = useState('');
  const [savingEdit, setSavingEdit] = useState(false);
  const [actionsDialogOpen, setActionsDialogOpen] = useState(false);
  const [actionsPhrase, setActionsPhrase] = useState<Phrase | null>(null);
  const [loading, setLoading] = useState(false);

  const loadPhrases = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('limit', '2000');
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
  }, []);

  const newPhrasesCount = phrases.filter((p) => p.is_published === 0).length;
  const publishedPhrasesCount = phrases.filter((p) => p.is_published === 1).length;

  const filteredPhrases = phrases
    .filter((phrase) => {
      if (statusFilter === 'all') return true;
      return String(phrase.is_published) === statusFilter;
    })
    .filter((phrase) =>
      !searchQuery.trim() || phrase.text_body.toLowerCase().includes(searchQuery.toLowerCase())
    )
    .sort((a, b) => {
      if (sortBy === 'text') {
        const left = a.text_body.toLowerCase();
        const right = b.text_body.toLowerCase();
        const cmp = left.localeCompare(right, 'ru');
        return sortDirection === 'asc' ? cmp : -cmp;
      }
      const left = new Date(a.created_at || 0).getTime();
      const right = new Date(b.created_at || 0).getTime();
      return sortDirection === 'asc' ? left - right : right - left;
    });

  const toggleSort = (field: 'text' | 'created_at') => {
    if (sortBy === field) {
      setSortDirection((prev) => (prev === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortBy(field);
    setSortDirection(field === 'text' ? 'asc' : 'desc');
  };

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

  const handleSingleDelete = async (id: number) => {
    try {
      await api(`/api/phrases/${id}`, 'DELETE', {});
      toast.success('Фраза удалена');
      if (selectedPhrases.includes(id)) {
        setSelectedPhrases((prev) => prev.filter((item) => item !== id));
      }
      await loadPhrases();
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const openActionsDialog = (phrase: Phrase) => {
    setActionsPhrase(phrase);
    setActionsDialogOpen(true);
  };

  const handleSingleStatus = async (id: number, isPublished: number) => {
    try {
      await api('/api/phrases/bulk-status', 'PUT', { ids: [id], is_published: isPublished });
      toast.success(isPublished === 1 ? 'Фраза отмечена опубликованной' : 'Фраза возвращена в новые');
      await loadPhrases();
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const openEditDialog = (phrase: Phrase) => {
    setEditingPhraseId(phrase.id);
    setEditingText(phrase.text_body || '');
    setEditingAuthor(phrase.author || '');
    setEditDialogOpen(true);
  };

  const handleSaveEdit = async () => {
    if (!editingPhraseId) return;
    const text = editingText.trim();
    if (!text) {
      toast.error('Текст фразы не может быть пустым');
      return;
    }
    setSavingEdit(true);
    try {
      await api(`/api/phrases/${editingPhraseId}`, 'PUT', { text_body: text, author: editingAuthor.trim() });
      toast.success('Фраза обновлена');
      setEditDialogOpen(false);
      setEditingPhraseId(null);
      setEditingText('');
      setEditingAuthor('');
      await loadPhrases();
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setSavingEdit(false);
    }
  };

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

      <div className="flex flex-wrap gap-2">
        <Button
          size="sm"
          variant={statusFilter === 'all' ? 'default' : 'outline'}
          onClick={() => setStatusFilter('all')}
          className={statusFilter === 'all' ? 'bg-blue-600 hover:bg-blue-700 text-white' : 'bg-zinc-900 border-zinc-700 text-zinc-200'}
        >
          Все ({phrases.length})
        </Button>
        <Button
          size="sm"
          variant={statusFilter === '0' ? 'default' : 'outline'}
          onClick={() => setStatusFilter('0')}
          className={statusFilter === '0' ? 'bg-yellow-600 hover:bg-yellow-700 text-white' : 'bg-zinc-900 border-zinc-700 text-zinc-200'}
        >
          Новые ({newPhrasesCount})
        </Button>
        <Button
          size="sm"
          variant={statusFilter === '1' ? 'default' : 'outline'}
          onClick={() => setStatusFilter('1')}
          className={statusFilter === '1' ? 'bg-green-600 hover:bg-green-700 text-white' : 'bg-zinc-900 border-zinc-700 text-zinc-200'}
        >
          Опубликованные ({publishedPhrasesCount})
        </Button>
      </div>

      <div className="flex flex-col gap-4 sm:flex-row bg-zinc-900 p-4 rounded-lg border border-zinc-800">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-500" />
          <Input placeholder="Поиск по тексту..." value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} className="pl-9 bg-zinc-800 border-zinc-700 text-zinc-100 placeholder:text-zinc-500" />
        </div>
        <Button variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-200" onClick={loadPhrases}>Поиск</Button>
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
              <TableHead className="text-zinc-300 min-w-[560px]">
                <button type="button" className="inline-flex items-center gap-2 hover:text-zinc-100" onClick={() => toggleSort('text')}>
                  Текст
                  {sortBy === 'text' ? (sortDirection === 'asc' ? '↑' : '↓') : ''}
                </button>
              </TableHead>
              <TableHead className="text-zinc-300">Статус</TableHead>
              <TableHead className="text-zinc-300">
                <button type="button" className="inline-flex items-center gap-2 hover:text-zinc-100" onClick={() => toggleSort('created_at')}>
                  Дата добавления
                  {sortBy === 'created_at' ? (sortDirection === 'asc' ? '↑' : '↓') : ''}
                </button>
              </TableHead>
              <TableHead className="w-12"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow><TableCell colSpan={5} className="text-center py-12 text-zinc-500">Загрузка...</TableCell></TableRow>
            ) : filteredPhrases.length === 0 ? (
              <TableRow><TableCell colSpan={5} className="text-center py-12 text-zinc-500">Фразы не найдены</TableCell></TableRow>
            ) : (
              filteredPhrases.map((phrase) => (
                <TableRow key={phrase.id} className="hover:bg-zinc-800/50 border-zinc-800">
                  <TableCell><Checkbox checked={selectedPhrases.includes(phrase.id)} onCheckedChange={(checked) => handleSelectPhrase(phrase.id, checked as boolean)} className="border-zinc-700" /></TableCell>
                  <TableCell className="text-zinc-200 whitespace-normal break-words leading-6">
                    <div>{phrase.text_body}</div>
                    {phrase.author ? <div className="mt-1 text-zinc-400 text-sm">— {phrase.author}</div> : null}
                  </TableCell>
                  <TableCell>
                    <Badge variant={phrase.is_published === 1 ? 'default' : 'secondary'} className={phrase.is_published === 1 ? 'bg-green-950/30 text-green-400 border-green-900/50' : 'bg-yellow-950/30 text-yellow-400 border-yellow-900/50'}>
                      {phrase.is_published === 1 ? 'Опубликована' : 'Новая'}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-zinc-400">{(phrase.created_at || '').slice(0, 10)}</TableCell>
                  <TableCell>
                    <div className="flex items-center justify-end gap-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="text-blue-300 hover:text-blue-200 hover:bg-zinc-800"
                        title="Генерировать пост"
                        onClick={() => handleGenerateForPhrase(phrase.id)}
                      >
                        <WandSparkles className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="text-zinc-400 hover:text-zinc-200"
                        onClick={() => openActionsDialog(phrase)}
                      >
                        <MoreVertical className="h-4 w-4" />
                      </Button>
                    </div>
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

      <Dialog open={editDialogOpen} onOpenChange={setEditDialogOpen}>
        <DialogContent className="bg-zinc-900 border-zinc-800 text-zinc-100">
          <DialogHeader>
            <DialogTitle>Редактировать фразу</DialogTitle>
            <DialogDescription className="text-zinc-400">
              Измените текст фразы и сохраните.
            </DialogDescription>
          </DialogHeader>
          <Textarea
            value={editingText}
            onChange={(e) => setEditingText(e.target.value)}
            rows={6}
            className="bg-zinc-800 border-zinc-700 text-zinc-100 placeholder:text-zinc-500"
          />
          <Input
            value={editingAuthor}
            onChange={(e) => setEditingAuthor(e.target.value)}
            placeholder="Автор (опционально)"
            className="bg-zinc-800 border-zinc-700 text-zinc-100 placeholder:text-zinc-500"
          />
          <DialogFooter>
            <Button variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-200" onClick={() => setEditDialogOpen(false)}>
              Отмена
            </Button>
            <Button className="bg-blue-600 hover:bg-blue-700 text-white" onClick={handleSaveEdit} disabled={savingEdit}>
              {savingEdit ? 'Сохранение...' : 'Сохранить'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={actionsDialogOpen}
        onOpenChange={(open) => {
          setActionsDialogOpen(open);
          if (!open) setActionsPhrase(null);
        }}
      >
        <DialogContent className="bg-zinc-900 border-zinc-800 text-zinc-100">
          <DialogHeader>
            <DialogTitle>Действия с фразой</DialogTitle>
            <DialogDescription className="text-zinc-400">
              Выберите действие для выбранной фразы.
            </DialogDescription>
          </DialogHeader>
          <div className="grid grid-cols-1 gap-2">
            <Button
              variant="outline"
              className="bg-zinc-800 border-zinc-700 text-zinc-200 justify-start"
              onClick={() => {
                if (!actionsPhrase) return;
                setActionsDialogOpen(false);
                handleGenerateForPhrase(actionsPhrase.id);
              }}
            >
              Генерировать пост
            </Button>
            <Button
              variant="outline"
              className="bg-zinc-800 border-zinc-700 text-zinc-200 justify-start"
              onClick={() => {
                if (!actionsPhrase) return;
                setActionsDialogOpen(false);
                openEditDialog(actionsPhrase);
              }}
            >
              Редактировать
            </Button>
            <Button
              variant="outline"
              className="bg-zinc-800 border-zinc-700 text-zinc-200 justify-start"
              onClick={() => {
                if (!actionsPhrase) return;
                const nextStatus = actionsPhrase.is_published === 1 ? 0 : 1;
                setActionsDialogOpen(false);
                handleSingleStatus(actionsPhrase.id, nextStatus);
              }}
            >
              {actionsPhrase?.is_published === 1
                ? 'Сменить статус на "Новая"'
                : 'Сменить статус на "Опубликованная"'}
            </Button>
            <Button
              variant="outline"
              className="bg-red-950/30 border-red-900/50 text-red-400 hover:bg-red-900/40 justify-start"
              onClick={() => {
                if (!actionsPhrase) return;
                setActionsDialogOpen(false);
                handleSingleDelete(actionsPhrase.id);
              }}
            >
              Удалить
            </Button>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              className="bg-zinc-800 border-zinc-700 text-zinc-200"
              onClick={() => setActionsDialogOpen(false)}
            >
              Закрыть
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
