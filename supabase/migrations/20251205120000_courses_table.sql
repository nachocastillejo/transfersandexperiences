-- Tabla para almacenar cursos sincronizados desde Google Sheets
-- Permite lecturas rápidas sin depender de la API de Google Sheets

create table if not exists public.courses (
  id uuid primary key default gen_random_uuid(),
  
  -- Identificadores únicos
  codigo text,                          -- Código del curso (ej: "COMM001")
  sheet_name text not null,             -- Nombre de la hoja origen (ej: "MADRID", "ANDALUCIA")
  row_hash text,                        -- Hash de la fila para detectar cambios
  
  -- Campos principales del curso
  curso text,                           -- Nombre del curso
  modalidad text,                       -- Online, Presencial, Aula Virtual
  fecha_inicio text,                    -- Fecha de inicio (texto como viene del sheet)
  horas text,                           -- Duración en horas
  lugar text,                           -- Lugar de impartición
  horario text,                         -- Horario del curso
  practicas text,                       -- Información sobre prácticas
  localizacion text,                    -- Dirección/localización
  zona text,                            -- Localidad/zona
  
  -- Campos de filtrado
  situacion_laboral text,               -- Ocupado, Desempleado, Autónomo, etc.
  requisitos_academicos text,           -- Nivel de formación requerido
  sector text,                          -- Sector profesional
  status text,                          -- Estado (pausado, activo, etc.)
  
  -- Puntuaciones para ordenamiento
  pp integer default 0,                 -- Puntuación PP
  pc integer default 0,                 -- Puntuación PC
  
  -- Contenido descriptivo
  que_aprenderas text,                  -- Descripción de contenidos
  salidas_profesionales text,           -- Salidas laborales
  
  -- Enlaces
  enlace text,                          -- URL de inscripción/información
  
  -- Campos adicionales (para cualquier columna extra del sheet)
  extra_data jsonb default '{}'::jsonb,
  
  -- Metadatos de sincronización
  phone_number_id text,                 -- Para multi-tenant
  synced_at timestamptz default now(),  -- Última sincronización
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  
  -- Constraint único: mismo código + sheet + phone_number_id
  unique (codigo, sheet_name, phone_number_id)
);

-- Índices para búsquedas rápidas
create index if not exists idx_courses_phone_sheet on public.courses (phone_number_id, sheet_name);
create index if not exists idx_courses_codigo on public.courses (codigo);
create index if not exists idx_courses_modalidad on public.courses (modalidad);
create index if not exists idx_courses_sector on public.courses (sector);
create index if not exists idx_courses_situacion on public.courses (situacion_laboral);
create index if not exists idx_courses_status on public.courses (status);
create index if not exists idx_courses_pp_pc on public.courses (pp desc, pc desc);
create index if not exists idx_courses_synced_at on public.courses (synced_at);

-- Índice GIN para búsqueda en extra_data
create index if not exists idx_courses_extra_data on public.courses using gin (extra_data);

-- Índice de texto completo para búsqueda de cursos
create index if not exists idx_courses_curso_fts on public.courses using gin (to_tsvector('spanish', coalesce(curso, '') || ' ' || coalesce(que_aprenderas, '')));

-- Trigger para updated_at
create or replace function public.set_courses_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end $$;

drop trigger if exists trg_courses_updated_at on public.courses;
create trigger trg_courses_updated_at
before update on public.courses
for each row execute function public.set_courses_updated_at();

-- RLS policies
alter table public.courses enable row level security;

-- Política para service role (acceso completo)
drop policy if exists "service_role_all_courses" on public.courses;
create policy "service_role_all_courses" on public.courses
for all using (true) with check (true);

-- Política para usuarios autenticados (solo lectura)
drop policy if exists "authenticated_read_courses" on public.courses;
create policy "authenticated_read_courses" on public.courses
for select using (auth.role() = 'authenticated');

-- Añadir a realtime (opcional, para futuras funcionalidades)
-- alter publication supabase_realtime add table public.courses;

comment on table public.courses is 'Cursos sincronizados desde Google Sheets para lecturas rápidas';
comment on column public.courses.row_hash is 'MD5 hash del contenido de la fila para detectar cambios';
comment on column public.courses.extra_data is 'Campos adicionales del sheet no mapeados a columnas específicas';












