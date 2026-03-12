import { useEffect, useState } from 'react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Textarea } from '../components/ui/textarea';
import { Label } from '../components/ui/label';
import { Card } from '../components/ui/card';
import { Checkbox } from '../components/ui/checkbox';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../components/ui/dialog';
import { Search, Calendar as CalendarIcon, Loader2, Send, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import { api, mskIso } from '../lib/api';

interface Film {
  id: number;
  title: string;
  year?: number;
  country?: string;
  description?: string;
  tags?: string;
}

interface Post {
  id: number;
  title: string;
  text_body: string;
  final_image_url?: string | null;
}

export function MoviesPage() {
  const [movies, setMovies] = useState<Film[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedMovie, setSelectedMovie] = useState<Film | null>(null);
  const [generateDialogOpen, setGenerateDialogOpen] = useState(false);

  const [post, setPost] = useState<Post | null>(null);
  const [generatingText, setGeneratingText] = useState(false);
  const [generatingImage, setGeneratingImage] = useState(false);
  const [postText, setPostText] = useState('');
  const [imageUrl, setImageUrl] = useState('');

  const [channels, setChannels] = useState({ telegram: true, vk: false, max: false, ok: false, pinterest: false, instagram: false });
  const [scheduleType, setScheduleType] = useState<'now' | 'scheduled'>('now');
  const [scheduleDate, setScheduleDate] = useState('');
  const [scheduleTime, setScheduleTime] = useState('');

  const loadFilms = async () => {
    try {
      const qs = searchQuery.trim() ? `?search=${encodeURIComponent(searchQuery.trim())}` : '';
      const rows = await api<Film[]>(`/api/films${qs}`);
      setMovies(rows);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  useEffect(() => { loadFilms(); }, []);

  const handleSelectMovie = async (movie: Film) => {
    setSelectedMovie(movie);
    try {
      const p = await api<Post>(`/api/films/${movie.id}/create-post`, 'POST', {});
      setPost(p);
      setPostText(p.text_body || '');
      setImageUrl(p.final_image_url || '');
      setGenerateDialogOpen(true);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const handleGenerateText = async () => {
    if (!post) return;
    setGeneratingText(true);
    try {
      const p = await api<Post>(`/api/posts/${post.id}/regenerate`, 'POST', { target: 'text', instruction: 'Сделай текст более интересным для соцсетей' });
      setPost(p);
      setPostText(p.text_body || '');
      toast.success('Текст обновлён');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingText(false);
    }
  };

  const handleGenerateImage = async () => {
    if (!post) return;
    setGeneratingImage(true);
    try {
      const p = await api<Post>(`/api/posts/${post.id}/preview`, 'POST', { scenario: 'Киноафиша, реалистично, атмосферно', regen_instruction: '' });
      setPost(p);
      setImageUrl(p.final_image_url || '');
      setPostText(p.text_body || postText);
      toast.success('Изображение сгенерировано');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingImage(false);
    }
  };

  const handleRegenerateAll = async () => {
    if (!post) return;
    setGeneratingText(true);
    setGeneratingImage(true);
    try {
      const p1 = await api<Post>(`/api/posts/${post.id}/regenerate`, 'POST', { target: 'both', instruction: 'Сделай сильнее эмоциональный отклик' });
      setPost(p1);
      const p2 = await api<Post>(`/api/posts/${post.id}/preview`, 'POST', { scenario: 'Киноафиша, реалистично, атмосферно', regen_instruction: 'Новый вариант' });
      setPost(p2);
      setPostText(p2.text_body || '');
      setImageUrl(p2.final_image_url || '');
      toast.success('Пост пересобран');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingText(false);
      setGeneratingImage(false);
    }
  };

  const handlePublish = async () => {
    if (!post) return;
    const targets = Object.entries(channels).filter(([_, v]) => v).map(([k]) => k);
    if (!targets.length) return toast.error('Выберите хотя бы один канал');
    try {
      if (scheduleType === 'scheduled') {
        const iso = mskIso(scheduleDate, scheduleTime);
        if (!iso) return toast.error('Заполни дату и время');
        await api(`/api/posts/${post.id}/publish`, 'POST', { mode: 'schedule', scheduled_for: iso });
      } else {
        if (targets.length === 1 && targets[0] === 'telegram') await api(`/api/posts/${post.id}/publish`, 'POST', { mode: 'now' });
        else await api(`/api/posts/${post.id}/publish/multi`, 'POST', { targets });
      }
      toast.success('Публикация отправлена');
      setGenerateDialogOpen(false);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400"><span>Главная</span><span>/</span><span className="text-zinc-200">Фильмы</span></div>
      <div><h1 className="text-3xl font-bold text-zinc-50">Фильмы</h1><p className="text-zinc-400 mt-1">Каталог фильмов для публикации</p></div>

      <div className="flex gap-2 bg-zinc-900 p-4 rounded-lg border border-zinc-800">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-500" />
          <Input placeholder="Поиск фильмов..." value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} className="pl-9 bg-zinc-800 border-zinc-700 text-zinc-100" />
        </div>
        <Button variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-200" onClick={loadFilms}>Поиск</Button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
        {movies.length === 0 ? <div className="col-span-full text-center py-12 text-zinc-500">Фильмы не найдены</div> : movies.map((movie) => (
          <Card key={movie.id} className="bg-zinc-900 border-zinc-800 overflow-hidden hover:border-zinc-700 transition-colors cursor-pointer group" onClick={() => handleSelectMovie(movie)}>
            <div className="aspect-[2/3] overflow-hidden bg-zinc-800 flex items-center justify-center text-zinc-500">{movie.title.slice(0, 1)}</div>
            <div className="p-4 space-y-2">
              <h3 className="text-lg font-semibold text-zinc-100 line-clamp-1">{movie.title}</h3>
              <div className="text-zinc-400 text-sm">{movie.year || '—'} · {movie.country || '—'}</div>
              <p className="text-zinc-400 text-sm line-clamp-2">{movie.description || 'Описание не заполнено'}</p>
            </div>
          </Card>
        ))}
      </div>

      <Dialog open={generateDialogOpen} onOpenChange={setGenerateDialogOpen}>
        <DialogContent className="bg-zinc-900 border-zinc-800 max-w-4xl max-h-[90vh] overflow-y-auto">
          <DialogHeader><DialogTitle className="text-zinc-50">{selectedMovie?.title}</DialogTitle></DialogHeader>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-4">
              <Card className="bg-zinc-800 border-zinc-700 p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <Label className="text-zinc-200">Текст поста</Label>
                  <Button size="sm" variant="outline" onClick={handleGenerateText} disabled={generatingText} className="bg-zinc-900 border-zinc-600 text-zinc-300 hover:bg-zinc-800">{generatingText ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}</Button>
                </div>
                <Textarea value={postText} onChange={(e) => setPostText(e.target.value)} rows={8} className="bg-zinc-900 border-zinc-600 text-zinc-100 resize-none" />
              </Card>

              <Card className="bg-zinc-800 border-zinc-700 p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <Label className="text-zinc-200">Изображение</Label>
                  <Button size="sm" variant="outline" onClick={handleGenerateImage} disabled={generatingImage} className="bg-zinc-900 border-zinc-600 text-zinc-300 hover:bg-zinc-800">{generatingImage ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}</Button>
                </div>
                {generatingImage ? <div className="aspect-square bg-zinc-900 rounded-lg flex items-center justify-center"><Loader2 className="h-8 w-8 text-zinc-500 animate-spin" /></div> : imageUrl ? <img src={imageUrl} alt="Generated" className="w-full aspect-square object-cover rounded-lg" /> : <div className="aspect-square bg-zinc-900 rounded-lg" />}
              </Card>

              <Button onClick={handleRegenerateAll} className="w-full bg-blue-600 hover:bg-blue-700 text-white"><RefreshCw className="mr-2 h-4 w-4" />Перегенерировать всё</Button>
            </div>

            <div className="space-y-4">
              <Card className="bg-zinc-800 border-zinc-700 p-4">
                <Label className="text-zinc-200 mb-3 block">Превью поста</Label>
                <div className="relative aspect-square rounded-lg overflow-hidden">
                  {imageUrl ? <img src={imageUrl} alt="Preview" className="w-full h-full object-cover" /> : <div className="w-full h-full bg-zinc-900" />}
                  <div className="absolute inset-0 bg-gradient-to-b from-transparent to-black/80 flex items-end p-4"><p className="text-white text-sm leading-relaxed line-clamp-4">{postText}</p></div>
                </div>
              </Card>

              <Card className="bg-zinc-800 border-zinc-700 p-4 space-y-4">
                <Label className="text-zinc-200">Каналы</Label>
                <div className="space-y-2">
                  {Object.entries(channels).map(([channel, checked]) => (
                    <div key={channel} className="flex items-center space-x-2">
                      <Checkbox id={`movie-channel-${channel}`} checked={checked} onCheckedChange={(c) => setChannels({ ...channels, [channel]: c as boolean })} className="border-zinc-600" />
                      <Label htmlFor={`movie-channel-${channel}`} className="text-zinc-300 capitalize cursor-pointer">{channel}</Label>
                    </div>
                  ))}
                </div>

                <div className="space-y-2">
                  <div className="flex items-center space-x-2"><Checkbox id="movie-schedule-now" checked={scheduleType === 'now'} onCheckedChange={(c) => c && setScheduleType('now')} className="border-zinc-600" /><Label htmlFor="movie-schedule-now" className="text-zinc-300 cursor-pointer">Опубликовать сейчас</Label></div>
                  <div className="flex items-center space-x-2"><Checkbox id="movie-schedule-later" checked={scheduleType === 'scheduled'} onCheckedChange={(c) => c && setScheduleType('scheduled')} className="border-zinc-600" /><Label htmlFor="movie-schedule-later" className="text-zinc-300 cursor-pointer">Запланировать</Label></div>
                  {scheduleType === 'scheduled' && <div className="grid grid-cols-2 gap-2 pt-2"><Input type="date" value={scheduleDate} onChange={(e) => setScheduleDate(e.target.value)} className="bg-zinc-900 border-zinc-600 text-zinc-100" /><Input type="time" value={scheduleTime} onChange={(e) => setScheduleTime(e.target.value)} className="bg-zinc-900 border-zinc-600 text-zinc-100" /></div>}
                </div>

                <Button onClick={handlePublish} className="w-full bg-green-600 hover:bg-green-700 text-white">{scheduleType === 'now' ? <><Send className="mr-2 h-4 w-4" />Опубликовать</> : <><CalendarIcon className="mr-2 h-4 w-4" />Запланировать</>}</Button>
              </Card>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
